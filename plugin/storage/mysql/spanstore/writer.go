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

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/mysql/config"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/jaegertracing/jaeger/plugin/storage/mysql/spanstore/dbmodel"
)


const (
	insertSpan = `INSERT INTO traces(trace_id, span_id, span_hash, parent_id, operation_name, flags,
				    start_time, duration, tags, logs, refs, process, service_name)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	insertServiceName = `INSERT INTO service_names(service_name) VALUES (?)`
	insertOperationName = `INSERT INTO operation_names(service_name, operation_name) VALUES (?, ?)`
	queryTraceByTraceId = `SELECT trace_id,span_id,operation_name,refs,flags,start_time,duration,tags,logs,process FROM traces where trace_id = ?`
	queryServiceNames = `SELECT service_name FROM service_names`
	queryOperationsByServiceName = `SELECT operation_name FROM operation_names where service_name = ?`
	defaultQuery = `SELECT trace_id FROM traces order by start_time limit 1`
)

var errTraceNotFound = errors.New("trace was not found")

// Store is an in-memory store of traces
type Store struct {
	mysql_client  *sql.DB
	config        config.Configuration
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
	return &Store{mysql_client: db, config: configuration}
}

// WriteSpan writes the given span
func (m *Store) WriteSpan(span *model.Span) error {
	ds := dbmodel.FromDomain(span)
	_, err := m.mysql_client.Exec(insertSpan,
		ds.TraceID,
		ds.SpanID,
		ds.SpanHash,
		ds.ParentID,
		ds.OperationName,
		ds.Flags,
		ds.StartTime,
		ds.Duration,
		ds.Tags,
		ds.Logs,
		ds.Refs,   // span 引用关系，是否是子span。有parentSpan
		ds.Process,
		span.Process.ServiceName)
	if err != nil {
		fmt.Println("write span err", zap.Error(err))
	}
	// write service_name TODO: 这里逻辑不好，应该先查询，没有的话，就新增
	_, err = m.mysql_client.Exec(insertServiceName, span.Process.ServiceName)
	if err != nil {
		fmt.Println("write service_name err", zap.Error(err))
	}

	// write operation_name
	_, err = m.mysql_client.Exec(insertOperationName, span.Process.ServiceName, span.OperationName)
	if err != nil {
		fmt.Println("write operation_name err", zap.Error(err))
	}

	return nil
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
	// ServiceName   string
	// OperationName string
	// Tags          map[string]string
	// StartTimeMin  time.Time
	// StartTimeMax  time.Time
	// DurationMin   time.Duration
	// DurationMax   time.Duration
	// NumTraces     int
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
