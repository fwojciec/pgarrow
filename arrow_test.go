package pgarrow_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOIDToArrowTypeConsistency ensures that CreateSchema produces Arrow types
// that exactly match what CompiledSchema expects. This catches subtle issues
// like missing TimeZone fields that could cause type mismatches.
func TestOIDToArrowTypeConsistency(t *testing.T) {
	t.Parallel()

	// Test all supported PostgreSQL OIDs
	testCases := []struct {
		name string
		oid  uint32
	}{
		{"bool", pgarrow.TypeOIDBool},
		{"bytea", pgarrow.TypeOIDBytea},
		{"int2", pgarrow.TypeOIDInt2},
		{"int4", pgarrow.TypeOIDInt4},
		{"int8", pgarrow.TypeOIDInt8},
		{"float4", pgarrow.TypeOIDFloat4},
		{"float8", pgarrow.TypeOIDFloat8},
		{"text", pgarrow.TypeOIDText},
		{"varchar", pgarrow.TypeOIDVarchar},
		{"bpchar", pgarrow.TypeOIDBpchar},
		{"name", pgarrow.TypeOIDName},
		{"char", pgarrow.TypeOIDChar},
		{"date", pgarrow.TypeOIDDate},
		{"time", pgarrow.TypeOIDTime},
		{"timestamp", pgarrow.TypeOIDTimestamp},     // This should catch the timezone issue
		{"timestamptz", pgarrow.TypeOIDTimestamptz}, // This too
		{"interval", pgarrow.TypeOIDInterval},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Create a schema using CreateSchema (which uses oidToArrowType)
			columns := []pgarrow.ColumnInfo{
				{Name: "test_col", OID: tc.oid},
			}

			schema, err := pgarrow.CreateSchema(columns)
			require.NoError(t, err, "CreateSchema should succeed for OID %d (%s)", tc.oid, tc.name)
			require.Equal(t, 1, schema.NumFields(), "Schema should have exactly one field")

			createSchemaType := schema.Field(0).Type

			// Create a CompiledSchema to see what type it expects
			compiledSchema, err := pgarrow.CompileSchema([]uint32{tc.oid}, schema, memory.DefaultAllocator)
			require.NoError(t, err, "CompileSchema should succeed for OID %d (%s)", tc.oid, tc.name)
			defer compiledSchema.Release()

			// Verify that the types are exactly equal
			// This will catch issues like missing TimeZone fields
			assert.True(t, arrow.TypeEqual(createSchemaType, schema.Field(0).Type),
				"Arrow types should be exactly equal for OID %d (%s).\nCreateSchema type: %s\nExpected type: %s",
				tc.oid, tc.name, createSchemaType, schema.Field(0).Type)
		})
	}
}

// TestTimestampTypeFields specifically tests that timestamp types have the correct TimeZone fields
func TestTimestampTypeFields(t *testing.T) {
	t.Parallel()

	t.Run("timestamp without timezone", func(t *testing.T) {
		t.Parallel()
		columns := []pgarrow.ColumnInfo{
			{Name: "ts", OID: pgarrow.TypeOIDTimestamp},
		}

		schema, err := pgarrow.CreateSchema(columns)
		require.NoError(t, err)

		timestampType, ok := schema.Field(0).Type.(*arrow.TimestampType)
		require.True(t, ok, "Should be a TimestampType")
		assert.Equal(t, arrow.Microsecond, timestampType.Unit, "Should use microsecond precision")
		assert.Empty(t, timestampType.TimeZone, "Should have empty timezone (not nil/unset)")
	})

	t.Run("timestamptz with timezone", func(t *testing.T) {
		t.Parallel()
		columns := []pgarrow.ColumnInfo{
			{Name: "tstz", OID: pgarrow.TypeOIDTimestamptz},
		}

		schema, err := pgarrow.CreateSchema(columns)
		require.NoError(t, err)

		timestampType, ok := schema.Field(0).Type.(*arrow.TimestampType)
		require.True(t, ok, "Should be a TimestampType")
		assert.Equal(t, arrow.Microsecond, timestampType.Unit, "Should use microsecond precision")
		assert.Equal(t, "UTC", timestampType.TimeZone, "Should have UTC timezone")
	})
}
