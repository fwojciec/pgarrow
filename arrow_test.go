// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pgarrow_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		columns  []pgarrow.ColumnInfo
		expected *arrow.Schema
	}{
		{
			name: "basic types schema",
			columns: []pgarrow.ColumnInfo{
				{Name: "id", OID: pgarrow.TypeOIDInt4},
				{Name: "name", OID: pgarrow.TypeOIDText},
				{Name: "active", OID: pgarrow.TypeOIDBool},
			},
			expected: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			}, nil),
		},
		{
			name: "all supported types",
			columns: []pgarrow.ColumnInfo{
				{Name: "col_bool", OID: pgarrow.TypeOIDBool},
				{Name: "col_int2", OID: pgarrow.TypeOIDInt2},
				{Name: "col_int4", OID: pgarrow.TypeOIDInt4},
				{Name: "col_int8", OID: pgarrow.TypeOIDInt8},
				{Name: "col_float4", OID: pgarrow.TypeOIDFloat4},
				{Name: "col_float8", OID: pgarrow.TypeOIDFloat8},
				{Name: "col_text", OID: pgarrow.TypeOIDText},
			},
			expected: arrow.NewSchema([]arrow.Field{
				{Name: "col_bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
				{Name: "col_int2", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
				{Name: "col_int4", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: "col_int8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "col_float4", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
				{Name: "col_float8", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				{Name: "col_text", Type: arrow.BinaryTypes.String, Nullable: true},
			}, nil),
		},
		{
			name:     "empty schema",
			columns:  []pgarrow.ColumnInfo{},
			expected: arrow.NewSchema([]arrow.Field{}, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			schema, err := pgarrow.CreateSchema(tt.columns)
			require.NoError(t, err)

			assert.True(t, schema.Equal(tt.expected))
		})
	}
}

func TestCreateSchemaUnsupportedType(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	columns := []pgarrow.ColumnInfo{
		{Name: "unsupported", OID: 9999}, // Invalid OID
	}

	_, err := pgarrow.CreateSchema(columns)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type OID")
}

func TestNewRecordBuilder(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	builder, err := pgarrow.NewRecordBuilder(schema, alloc)
	require.NoError(t, err)
	defer builder.Release()

	assert.NotNil(t, builder)
	assert.True(t, builder.Schema().Equal(schema))
}

func TestRecordBuilderAppendRow(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col_bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "col_int2", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "col_int4", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "col_int8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "col_float4", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "col_float8", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "col_text", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	builder, err := pgarrow.NewRecordBuilder(schema, alloc)
	require.NoError(t, err)
	defer builder.Release()

	// Test row with all values
	err = builder.AppendRow([]interface{}{
		true,             // bool
		int16(100),       // int2
		int32(1000),      // int4
		int64(10000),     // int8
		float32(3.14),    // float4
		float64(2.71828), // float8
		"hello",          // text
	})
	require.NoError(t, err)

	// Test row with NULL values
	err = builder.AppendRow([]interface{}{
		nil, // NULL bool
		nil, // NULL int2
		nil, // NULL int4
		nil, // NULL int8
		nil, // NULL float4
		nil, // NULL float8
		nil, // NULL text
	})
	require.NoError(t, err)

	record, err := builder.NewRecord()
	require.NoError(t, err)
	defer record.Release()

	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, int64(7), record.NumCols())
}

func TestRecordBuilderAppendRowTypeMismatch(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	builder, err := pgarrow.NewRecordBuilder(schema, alloc)
	require.NoError(t, err)
	defer builder.Release()

	// Wrong type - string instead of int32
	err = builder.AppendRow([]interface{}{"not_an_int"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
}

func TestRecordBuilderAppendRowWrongLength(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	builder, err := pgarrow.NewRecordBuilder(schema, alloc)
	require.NoError(t, err)
	defer builder.Release()

	// Too few values
	err = builder.AppendRow([]interface{}{42})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 2 values, got 1")

	// Too many values
	err = builder.AppendRow([]interface{}{42, "test", "extra"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 2 values, got 3")
}

func TestRecordBuilderNullHandling(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col_bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "col_int2", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "col_int4", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "col_int8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "col_float4", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "col_float8", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "col_text", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	builder, err := pgarrow.NewRecordBuilder(schema, alloc)
	require.NoError(t, err)
	defer builder.Release()

	// All NULL values
	err = builder.AppendRow([]interface{}{nil, nil, nil, nil, nil, nil, nil})
	require.NoError(t, err)

	// Mixed NULL and non-NULL values
	err = builder.AppendRow([]interface{}{
		true,         // bool
		nil,          // NULL int2
		int32(42),    // int4
		nil,          // NULL int8
		float32(1.0), // float4
		nil,          // NULL float8
		"test",       // text
	})
	require.NoError(t, err)

	record, err := builder.NewRecord()
	require.NoError(t, err)
	defer record.Release()

	assert.Equal(t, int64(2), record.NumRows())

	// Check first row (all NULLs)
	for i := 0; i < int(record.NumCols()); i++ {
		assert.False(t, record.Column(i).IsValid(0), "Column %d should be NULL at row 0", i)
	}

	// Check second row (mixed values)
	assert.True(t, record.Column(0).IsValid(1))  // bool is valid
	assert.False(t, record.Column(1).IsValid(1)) // int2 is NULL
	assert.True(t, record.Column(2).IsValid(1))  // int4 is valid
	assert.False(t, record.Column(3).IsValid(1)) // int8 is NULL
	assert.True(t, record.Column(4).IsValid(1))  // float4 is valid
	assert.False(t, record.Column(5).IsValid(1)) // float8 is NULL
	assert.True(t, record.Column(6).IsValid(1))  // text is valid
}

func TestRecordBuilderNewRecord(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	builder, err := pgarrow.NewRecordBuilder(schema, alloc)
	require.NoError(t, err)
	defer builder.Release()

	// Add multiple rows
	for i := 0; i < 10; i++ {
		err = builder.AppendRow([]interface{}{
			int32(i),
			float64(i) * 3.14,
		})
		require.NoError(t, err)
	}

	record, err := builder.NewRecord()
	require.NoError(t, err)
	defer record.Release()

	// Validate record properties
	assert.True(t, record.Schema().Equal(schema))
	assert.Equal(t, int64(10), record.NumRows())
	assert.Equal(t, int64(2), record.NumCols())

	// Validate data integrity
	idCol, ok := record.Column(0).(*array.Int32)
	require.True(t, ok, "Column 0 should be *array.Int32")
	valueCol, ok := record.Column(1).(*array.Float64)
	require.True(t, ok, "Column 1 should be *array.Float64")

	for i := 0; i < 10; i++ {
		assert.True(t, idCol.IsValid(i))
		assert.True(t, valueCol.IsValid(i))
		assert.Equal(t, int32(i), idCol.Value(i))
		assert.InDelta(t, float64(i)*3.14, valueCol.Value(i), 0.001)
	}
}

func TestRecordBuilderEmptyRecord(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	builder, err := pgarrow.NewRecordBuilder(schema, alloc)
	require.NoError(t, err)
	defer builder.Release()

	// Create record without adding any rows
	record, err := builder.NewRecord()
	require.NoError(t, err)
	defer record.Release()

	assert.Equal(t, int64(0), record.NumRows())
	assert.Equal(t, int64(1), record.NumCols())
	assert.True(t, record.Schema().Equal(schema))
}

func TestRecordBuilderMultipleRecords(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	builder, err := pgarrow.NewRecordBuilder(schema, alloc)
	require.NoError(t, err)
	defer builder.Release()

	// Create first record
	err = builder.AppendRow([]interface{}{int32(1)})
	require.NoError(t, err)

	record1, err := builder.NewRecord()
	require.NoError(t, err)
	defer record1.Release()

	assert.Equal(t, int64(1), record1.NumRows())

	// Create second record (builder should be reset)
	err = builder.AppendRow([]interface{}{int32(2)})
	require.NoError(t, err)
	err = builder.AppendRow([]interface{}{int32(3)})
	require.NoError(t, err)

	record2, err := builder.NewRecord()
	require.NoError(t, err)
	defer record2.Release()

	assert.Equal(t, int64(2), record2.NumRows())
}

func TestRecordBuilderZeroColumns(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	// Create schema with zero columns (empty schema)
	schema := arrow.NewSchema([]arrow.Field{}, nil)

	builder, err := pgarrow.NewRecordBuilder(schema, alloc)
	require.NoError(t, err)
	defer builder.Release()

	// Create record with zero columns - this should not panic
	record, err := builder.NewRecord()
	require.NoError(t, err)
	defer record.Release()

	assert.Equal(t, int64(0), record.NumRows())
	assert.Equal(t, int64(0), record.NumCols())
	assert.True(t, record.Schema().Equal(schema))
}
