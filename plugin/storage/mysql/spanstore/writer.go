// Copyright (c) 2019 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spanstore

import (
	"context"
	"errors"
	"database/sql"
	"fmt"
	"strconv"
	"encoding/json"
	"time"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"github.com/smartwalle/dbs"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/mysql/config"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/jaegertracing/jaeger/plugin/storage/mysql/spanstore/dbmodel"
)


const (
	insertSpan = `INSERT INTO traces(trace_id, span_id, span_hash, parent_id, operation_name, flags,
				    start_time, duration, tags, logs, refs, process, service_name)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	insertServiceName = `INSERT ignore INTO service_names(service_name) VALUES (?)`
	insertOperationName = `INSERT ignore  INTO operation_names(service_name, operation_name) VALUES (?, ?)`
	queryTraceByTraceId = `SELECT trace_id,span_id,operation_name,refs,flags,start_time,duration,tags,logs,process FROM traces where trace_id = ?`
	queryServiceNames = `SELECT service_name FROM service_names`
	queryOperationsByServiceName = `SELECT operation_name FROM operation_names where service_name = ?`
	//defaultQuery = `SELECT trace_id FROM traces order by start_time limit 1`
)

var errTraceNotFound = errors.New("trace was not found")

// Store is an in-memory store of traces
type Store struct {
	mysql_client  *sql.DB
	config        config.Configuration
	eventQueue    chan *dbmodel.Span
	caches        map[string]map[string]struct{}
	cacheLock     sync.Mutex
}

// // NewStore creates a localhost store
// func NewStore() *Store {
// 	return WithConfiguration(config.Configuration{Url: "root:123@tcp()"})
// }

// Close closes SpanWriter
func (m *Store) Close() error {
	m.mysql_client.Close()
	return nil
}

// WithConfiguration creates a new in mysql storage based on the given configuration
func WithConfiguration(configuration config.Configuration) *Store {
	db, err := sql.Open("mysql", configuration.Url)
	if err != nil {
		fmt.Println("Cannot create mysql session", zap.Error(err))
		return nil
	}
	return &Store{mysql_client: db, config: configuration, eventQueue: make(chan *dbmodel.Span, 10000), caches: map[string]map[string]struct{}{}}
}

func (m *Store)Initialize(){
	m.load_caches()
	m.start_storage_backgroud()
}

func (m *Store)load_caches(){
	m.cacheLock.Lock()
	defer m.cacheLock.Unlock()
	service_names, err := m.getServices()
	if err != nil {
		fmt.Println("getServices error", zap.Error(err))
		return 
	}
	for _, service_name := range service_names {
		m.caches[service_name] = map[string]struct{}{}
		operation_names, err := m.getOperations(service_name)
		if err != nil {
			fmt.Println("get service operation error", zap.Error(err))
			continue 
		}
		for _, operation_name := range operation_names{
			m.caches[service_name][operation_name] = struct{}{}
		}
	}
	fmt.Printf("load caches success: %+v \n", m.caches)
}

func (m *Store)start_storage_backgroud(){
	var (
		eventQueue     = m.eventQueue
		batchSize      = 50 //m.batchSize
		workers        = 8 //m.workers
		lingerTime     = 200 * time.Millisecond //m.lingerTime
		batchProcessor = func(batch []*dbmodel.Span) error {
			if len(batch) > 0{
				fmt.Printf("process %d items \n", len(batch))
				m.batch_insert(batch)
			}else{
				fmt.Println("batch is 0")
			}
			return nil
		}
		errHandler = func(err error, batch []*dbmodel.Span) {
			fmt.Println("some error happens")
		}
	)

	for i := 0; i < workers; i++ {
		go func() {
			var batch []*dbmodel.Span
			lingerTimer := time.NewTimer(0)
			if !lingerTimer.Stop() {
				<-lingerTimer.C
			}
			defer lingerTimer.Stop()

			for {
				select {
				case msg := <-eventQueue:
					batch = append(batch, msg)
					if len(batch) != batchSize {
						if len(batch) == 1 {
							lingerTimer.Reset(lingerTime)
						}
						break
					}

					fmt.Println("batch is reach")
					if err := batchProcessor(batch); err != nil {
						errHandler(err, batch)
					}

					if !lingerTimer.Stop() {
						<-lingerTimer.C
					}

					batch = make([]*dbmodel.Span, 0)
				case <-lingerTimer.C:
					fmt.Println("time is reach")
					if err := batchProcessor(batch); err != nil {
						errHandler(err, batch)
					}

					batch = make([]*dbmodel.Span, 0)
				}
			}
		}()
		fmt.Printf("start storage worker %d success \n", i)
	}
}

func (m *Store)batch_insert(spans []*dbmodel.Span) {
    var ib = dbs.NewInsertBuilder()
    ib.Table("traces")
    ib.Columns("trace_id", "span_id", "span_hash", "parent_id", "operation_name", "flags",
		"start_time", "duration", "tags", "logs", "refs", "process", "service_name")
    for _, span := range spans {
		ib.Values(span.TraceID, span.SpanID,span.SpanHash, span.ParentID, span.OperationName, span.Flags, span.StartTime,
			span.Duration, span.Tags, span.Logs, span.Refs, span.Process, span.ServiceName)
    }
    ib.Exec(m.mysql_client)
}

// WriteSpan writes the given span
func (m *Store) WriteSpan(span *model.Span) error {
	ds := dbmodel.FromDomain(span)
	select {
	case m.eventQueue <- ds:
		fmt.Println("sent one span")
	default:
		fmt.Println("no span sent")
	}

	// use cache to save the less data, note to load the data to cache when start init 
	m.UpdateCaches(ds.ServiceName, ds.OperationName)

	return nil
}

func (m *Store) UpdateCaches(service string, operation string){
	m.cacheLock.Lock()
	defer m.cacheLock.Unlock()
	service_operations, ok := m.caches[service]
	if !ok {
		m.caches[service] = map[string]struct{}{}
		m.caches[service][operation] = struct{}{}
		// insert service operation to mysql
		_, err := m.mysql_client.Exec(insertServiceName, service)
		if err != nil {
			fmt.Println("write service_name err", zap.Error(err))
		}
	}else{
		if _, ok := service_operations[operation]; !ok{
			m.caches[service][operation] = struct{}{}
			// insert operation to mysql
			_, err := m.mysql_client.Exec(insertOperationName, service, operation)
			if err != nil {
				fmt.Println("write operation_name err", zap.Error(err))
			}
		}
	}
}

// GetTrace gets a trace
func (m *Store) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error){
	trace := model.Trace{}
	trace_id := traceID.String()
	rows, err := m.mysql_client.Query(queryTraceByTraceId, trace_id)
	if err != nil {
		fmt.Println("queryTrace err", zap.Error(err))
		return nil, err
	}
	defer rows.Close()
	var spans []*model.Span
	for rows.Next() {
		var span model.Span
		var trace_id,span_id,operation_name,refs,tags,logs,process string
		var flags,start_time,duration int
		var SpanId int
		err := rows.Scan(&trace_id, &span_id, &operation_name, &refs, &flags, &start_time, &duration, &tags, &logs, &process)
		if err != nil {
			fmt.Println("queryTrace scan err", zap.Error(err))
		}
		span.TraceID = traceID
		SpanId, err= strconv.Atoi(span_id)
		if err != nil {
			fmt.Println("queryTrace SpanId err", zap.Error(err))
		}else {
			span.SpanID = model.NewSpanID(uint64(SpanId))
		}
		span.OperationName = operation_name
		var refs1 []dbmodel.SpanRef
		err = json.Unmarshal([]byte(refs), &refs1)
		if err != nil {
			fmt.Println("queryTrace refs err", zap.Error(err))
		}else {
			span.References = dbmodel.ToDomainRefs(refs1, traceID)
		}
		err = json.Unmarshal([]byte(tags), &span.Tags)
		if err != nil {
			fmt.Println("queryTrace tags err", zap.Error(err))
		}
		err = json.Unmarshal([]byte(logs), &span.Logs)
		if err != nil {
			fmt.Println("queryTrace logs err", zap.Error(err))
		}
		err = json.Unmarshal([]byte(process), &span.Process)
		if err != nil {
			fmt.Println("queryTrace process err", zap.Error(err))
		}
		span.Flags = model.Flags(uint32(flags))
		span.StartTime = model.EpochMicrosecondsAsTime(uint64(start_time))
		span.Duration = model.MicrosecondsAsDuration(uint64(duration))
		spans = append(spans, &span)
	}
	trace.Spans = spans

	return &trace, nil
}

// GetServices returns a list of all known services
func (m *Store) GetServices(ctx context.Context) ([]string, error){
	return m.getServices()
}
 
func (m *Store) getServices()([]string, error){
	rows, err := m.mysql_client.Query(queryServiceNames)
	if err != nil {
		fmt.Println("queryService err", zap.Error(err))
		return nil, err
	}
	defer rows.Close()
	var service_names []string
	var service_name string
	for rows.Next() {
		err := rows.Scan(&service_name)
		if err != nil {
			fmt.Println("queryService scan err", zap.Error(err))
		}
		service_names = append(service_names, service_name)
	}
	return service_names, nil
}

// GetOperations returns the operations of a given service
func (m *Store) GetOperations(ctx context.Context, service string) ([]string, error){
	return m.getOperations(service)
}

func (m *Store) getOperations(service string) ([]string, error){
	rows, err := m.mysql_client.Query(queryOperationsByServiceName, service)
	if err != nil {
		fmt.Println("queryOperation err", zap.Error(err))
		return nil, err
	}
	defer rows.Close()
	var operation_names []string
	var operation_name string
	for rows.Next() {
		err := rows.Scan(&operation_name)
		if err != nil {
			fmt.Println("queryService scan err", zap.Error(err))
		}
		operation_names = append(operation_names, operation_name)
	}
	return operation_names, nil
}

// FindTraces returns all traces in the query parameters are satisfied by a trace's span
func (m *Store) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error){
	traceIds,err := m.FindTraceIDs(ctx, query)
	if err != nil {
		fmt.Println("queryTraces err", zap.Error(err))
		return nil, err
	}
	var traces []*model.Trace
	for _, trace_id := range traceIds {
		trace,err := m.GetTrace(ctx, trace_id)
		if err != nil {
			fmt.Println("queryTraces GetTrace err", zap.Error(err))
		}else {
			traces = append(traces, trace)
		}
	}

	return traces, nil
}

// FindTraceIDs is not implemented.
func (m *Store) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error){
	defaultQuery := gen_query_sql(query)
	rows, err := m.mysql_client.Query(defaultQuery)
	defer rows.Close()
	if err != nil {
		fmt.Println("queryTraceIDs err", zap.Error(err))
		return nil, err
	}
	var traceIds []model.TraceID
	var traceIdStr string
	for rows.Next() {
		err := rows.Scan(&traceIdStr)
		if err != nil {
			fmt.Println("queryTraceIDs scan err", zap.Error(err))
		}
		traceId, err := model.TraceIDFromString(traceIdStr)
		if err != nil {
			fmt.Println("queryTraceIDs TraceIDFromString err", zap.Error(err))
		}else {
			traceIds = append(traceIds, traceId)
		}
	}
	return traceIds, nil
}

func gen_query_sql (query *spanstore.TraceQueryParameters) string {
	defaultQuery := fmt.Sprintf("SELECT trace_id FROM traces WHERE service_name='%s'", query.ServiceName) 
	if query.OperationName != ""{
		defaultQuery = defaultQuery + fmt.Sprintf(" and operation_name='%s'", query.OperationName)
	}
	var t time.Time
	if query.StartTimeMax != t {
		start_time_max := int64(model.TimeAsEpochMicroseconds(query.StartTimeMax))
		defaultQuery = defaultQuery + fmt.Sprintf(" and start_time<=%d", start_time_max)
	}
	if query.StartTimeMin != t {
		start_time_min := int64(model.TimeAsEpochMicroseconds(query.StartTimeMin))
		defaultQuery = defaultQuery + fmt.Sprintf(" and start_time>=%d", start_time_min)
	}
	if query.DurationMax > 0 {
		duration_max := int64(model.DurationAsMicroseconds(query.DurationMax))
		defaultQuery = defaultQuery + fmt.Sprintf(" and duration<=%d", duration_max)
	}
	if query.DurationMin > 0 {
		duration_min := int64(model.DurationAsMicroseconds(query.DurationMin))
		defaultQuery = defaultQuery + fmt.Sprintf(" and duration>=%d", duration_min)
	}
	limit := query.NumTraces
	if limit <= 0 {
		limit = 20
	}
	defaultQuery = defaultQuery + fmt.Sprintf(" limit %d", limit)
	fmt.Println(defaultQuery)
	return defaultQuery
}