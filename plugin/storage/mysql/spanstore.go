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

package mysql

import (
	"errors"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/mysql/config"
	"github.com/jaegertracing/jaeger/plugin/storage/cassandra/spanstore/dbmodel"
)


const (
	insertSpan = `
		INSERT
		INTO traces(trace_id, span_id, span_hash, parent_id, operation_name, flags,
				    start_time, duration)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
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
	res, err := m.mysql_client.Exec(insertSpan,
		ds.TraceID.String(),
		ds.SpanID,
		ds.SpanHash,
		ds.ParentID,
		ds.OperationName,
		ds.Flags,
		ds.StartTime,
		ds.Duration,
		// ds.Tags,
		// ds.Logs,
		// ds.Refs,   // span 引用关系，是否是子span。有parentSpan
		// ds.Process,
	)
	
	if err != nil {
		fmt.Println("write span err", zap.Error(err))
	}
	fmt.Println(res)

	return nil
}

