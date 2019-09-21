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

package main

import (
	"context"
	"time"
	"database/sql"
	"strconv"
	"fmt"

	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"
	"github.com/go-sql-driver/mysql"

	"github.com/jaegertracing/jaeger/model"
	//mysqlcfg "github.com/jaegertracing/jaeger/pkg/mysql/config"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/jaegertracing/jaeger/plugin/storage/cassandra/spanstore/dbmodel"
)
const (
	insertSpan = `
		INSERT
		INTO traces(trace_id, span_id, span_hash, parent_id, operation_name, flags,
				    start_time, duration)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	insertTag = `
		INSERT
		INTO tag_index(trace_id, span_id, service_name, start_time, tag_key, tag_value)
		VALUES (?, ?, ?, ?, ?, ?)`

	serviceNameIndex = `
		INSERT
		INTO service_name_index(service_name, bucket, start_time, trace_id)
		VALUES (?, ?, ?, ?)`

	serviceOperationIndex = `
		INSERT
		INTO
		service_operation_index(service_name, operation_name, start_time, trace_id)
		VALUES (?, ?, ?, ?)`

	durationIndex = `
		INSERT
		INTO duration_index(service_name, operation_name, bucket, duration, start_time, trace_id)
		VALUES (?, ?, ?, ?, ?, ?)`

	maximumTagKeyOrValueSize = 256

	// DefaultNumBuckets Number of buckets for bucketed keys
	defaultNumBuckets = 10

	durationBucketSize = time.Hour
)

// {
// "trace-id":"3d742478634f4e90",
// "span-id":"6bc09d58293179cf",
// "OperationName":"GetDriver",
// //"References":[{"trace_id":"AAAAAAAAAAA9dCR4Y09OkA==","span_id":"IU2aN+9Y2vg="}],
// "Flags":1,
// "StartTime":1568858790.662019,
// "Duration":0.010627,
// //"Tags":[{"key":"param.driverID","v_str":"T714279C"},{"key":"span.kind","v_str":"client"},{"key":"internal.span.format","v_str":"proto"}],
// //"Logs":null,
// //"Process":"service_name:\"redis\" tags:<key:\"jaeger.version\" v_str:\"Go-2.15.1dev\" > tags:<key:\"hostname\" v_str:\"liyangjin-dev\" > tags:<key:\"ip\" v_str:\"10.5.27.82\" > tags:<key:\"client-uuid\" v_str:\"1b5dd22b138e3f49\" > ",
// //"ProcessID":"",
// //"Warnings":[]
// }


var logger, _ = zap.NewDevelopment()

// TODO: this is going morph into a load testing framework for cassandra 3.7
func main() {
	//noScope := metrics.NullFactory
	url := "pamc_online00:iI5nYfdXaFL89Ftv@tcp(10.5.29.126:3306)/go?charset=utf8"
	db, err := sql.Open("mysql", url)
	if err != nil {
		logger.Fatal("Cannot create Cassandra session", zap.Error(err))
	}

	// CREATE TABLE `traces` (
	// 	`trace_id` varchar(128) not null,
	// 	`span_id` varchar(128) not null,
	// 	`operation_name` varchar(128),
	// 	`flags` int(11),
	// 	`start_time` float,
	// 	`duration` float
	// )

	// CREATE TABLE IF NOT EXISTS traces (
	// 	trace_id        blob,
	// 	span_id         bigint,
	// 	span_hash       bigint,
	// 	parent_id       bigint,
	// 	operation_name  text,
	// 	flags           int(11),
	// 	start_time      bigint,
	// 	duration        bigint
	// )
	span := getSomeSpan()
	ds := dbmodel.FromDomain(span)
	res, err := db.Exec(insertSpan,
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
		logger.Fatal("err", zap.Error(err))
	}
	fmt.Println(res)
	db.Close()
	
	
	//fmt.Println(start_time, duration)



	// logger.Info("Span written to the storage by the collector",
	// zap.Stringer("trace-id", span.TraceID), zap.Stringer("span-id", span.SpanID),
	// zap.Any("OperationName", span.OperationName), zap.Any("References", span.References),
	// zap.Any("Flags", span.Flags), zap.Time("StartTime", span.StartTime.Unix()),
	// zap.Duration("Duration", span.Duration), zap.Any("Tags", span.Tags),
	// zap.Any("Logs", span.Logs), zap.Any("Process", span.Process),
	// zap.Any("ProcessID", span.ProcessID), zap.Any("Warnings", span.Warnings))
	

	// spanStore := db
	// if err = spanStore.WriteSpan(getSomeSpan()); err != nil {
	// 	logger.Fatal("Failed to save", zap.Error(err))
	// } else {
	// 	logger.Info("Saved span", zap.String("spanID", getSomeSpan().SpanID.String()))
	// }
	// s := getSomeSpan()
	// ctx := context.Background()
	// trace, err := spanReader.GetTrace(ctx, s.TraceID)
	// if err != nil {
	// 	logger.Fatal("Failed to read", zap.Error(err))
	// } else {
	// 	logger.Info("Loaded trace", zap.Any("trace", trace))
	// }

	// tqp := &spanstore.TraceQueryParameters{
	// 	ServiceName:  "someServiceName",
	// 	StartTimeMin: time.Now().Add(time.Hour * -1),
	// 	StartTimeMax: time.Now().Add(time.Hour),
	// }
	// logger.Info("Check main query")
	// queryAndPrint(ctx, spanReader, tqp)

	// tqp.OperationName = "opName"
	// logger.Info("Check query with operation")
	// queryAndPrint(ctx, spanReader, tqp)

	// tqp.Tags = map[string]string{
	// 	"someKey": "someVal",
	// }
	// logger.Info("Check query with operation name and tags")
	// queryAndPrint(ctx, spanReader, tqp)

	// tqp.DurationMin = 0
	// tqp.DurationMax = time.Hour
	// tqp.Tags = map[string]string{}
	// logger.Info("check query with duration")
	// queryAndPrint(ctx, spanReader, tqp)
}

// func queryAndPrint(ctx context.Context, spanReader *cSpanStore.SpanReader, tqp *spanstore.TraceQueryParameters) {
// 	traces, err := spanReader.FindTraces(ctx, tqp)
// 	if err != nil {
// 		logger.Fatal("Failed to query", zap.Error(err))
// 	} else {
// 		logger.Info("Found trace(s)", zap.Any("traces", traces))
// 	}
// }

func getSomeProcess() *model.Process {
	processTagVal := "indexMe"
	return &model.Process{
		ServiceName: "someServiceName",
		Tags: model.KeyValues{
			model.String("processTagKey", processTagVal),
		},
	}
}

func getSomeSpan() *model.Span {
	traceID := model.NewTraceID(1, 2)
	return &model.Span{
		TraceID:       traceID,
		SpanID:        model.NewSpanID(3),
		OperationName: "opName",
		References:    model.MaybeAddParentSpanID(traceID, 4, getReferences()),
		Flags:         model.Flags(uint32(5)),
		StartTime:     time.Now(),
		Duration:      50000 * time.Microsecond,
		Tags:          getTags(),
		Logs:          getLogs(),
		Process:       getSomeProcess(),
	}
}

func getReferences() []model.SpanRef {
	return []model.SpanRef{
		{
			RefType: model.ChildOf,
			TraceID: model.NewTraceID(1, 1),
			SpanID:  model.NewSpanID(4),
		},
	}
}

func getTags() model.KeyValues {
	someVal := "someVal"
	return model.KeyValues{
		model.String("someKey", someVal),
	}
}

func getLogs() []model.Log {
	logTag := "this is a msg"
	return []model.Log{
		{
			Timestamp: time.Now(),
			Fields: model.KeyValues{
				model.String("event", logTag),
			},
		},
	}
}
