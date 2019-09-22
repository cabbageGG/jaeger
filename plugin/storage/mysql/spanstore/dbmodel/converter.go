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

package dbmodel

import (
	"fmt"
	"encoding/json"

	"github.com/jaegertracing/jaeger/model"
)

var (
	dbToDomainRefMap = map[string]model.SpanRefType{
		childOf:     model.SpanRefType_CHILD_OF,
		followsFrom: model.SpanRefType_FOLLOWS_FROM,
	}

	domainToDBRefMap = map[model.SpanRefType]string{
		model.SpanRefType_CHILD_OF:     childOf,
		model.SpanRefType_FOLLOWS_FROM: followsFrom,
	}

	domainToDBValueTypeMap = map[model.ValueType]string{
		model.StringType:  stringType,
		model.BoolType:    boolType,
		model.Int64Type:   int64Type,
		model.Float64Type: float64Type,
		model.BinaryType:  binaryType,
	}
)

// FromDomain converts a domain model.Span to a database Span
func FromDomain(span *model.Span) *Span {
	return converter{}.fromDomain(span)
}

// converter converts Spans between domain and database representations.
// It primarily exists to namespace the conversion functions.
type converter struct{}

func (c converter) fromDomain(span *model.Span) *Span {
	tags := c.toDBTags(span.Tags)
	logs := c.toDBLogs(span.Logs)
	refs, parent_id := c.toDBRefs(span.References)
	udtProcess := c.toDBProcess(span.Process)
	spanHash, _ := model.HashCode(span)

	return &Span{
		TraceID:       span.SpanID.String(),
		SpanID:        int64(span.SpanID),
		ParentID:      parent_id,
		OperationName: span.OperationName,
		Flags:         int32(span.Flags),
		StartTime:     int64(model.TimeAsEpochMicroseconds(span.StartTime)),
		Duration:      int64(model.DurationAsMicroseconds(span.Duration)),
		Tags:          tags,
		Logs:          logs,
		Refs:          refs,
		Process:       udtProcess,
		ServiceName:   span.Process.ServiceName,
		SpanHash:      int64(spanHash),
	}
}

func jsonMarshal(v interface{}) string {
	data, err := json.Marshal(v)
	if err != nil {
		fmt.Println(err)
		return fmt.Sprintf("jsonMarshal Marshal error: %s", err)
	}
	return string(data)
}

func (c converter) toDBTags(tags []model.KeyValue) string {
	return jsonMarshal(tags)
}

func (c converter) toDBLogs(logs []model.Log) string {
	return jsonMarshal(logs)
}

func (c converter) toDBRefs(refs []model.SpanRef) (string, int64) {
	retMe := make([]SpanRef, len(refs))
	var parent_id int64
	for i, r := range refs {
		retMe[i] = SpanRef{
			TraceID: r.TraceID.String(),
			SpanID:  int64(r.SpanID),
			RefType: domainToDBRefMap[r.RefType],
		}
		if v := domainToDBRefMap[r.RefType]; v == childOf{
			parent_id = int64(r.SpanID)
		}
	}
	return jsonMarshal(retMe), parent_id
}

func (c converter) toDBProcess(process *model.Process) string {
	return jsonMarshal(process)
}
