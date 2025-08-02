package pgarrow

import "fmt"

// QueryError provides detailed context about query execution failures
type QueryError struct {
	SQL           string // The SQL query that failed
	ConnectionStr string // Connection string (sensitive parts masked)
	Operation     string // Which operation failed (e.g., "metadata_discovery", "copy_execution")
	Err           error  // The underlying error
}

func (e *QueryError) Error() string {
	sql := e.SQL
	if len(sql) > 100 {
		sql = sql[:100] + "..."
	}
	return fmt.Sprintf("query failed during %s: %v (SQL: %s)", e.Operation, e.Err, sql)
}

func (e *QueryError) Unwrap() error {
	return e.Err
}

// ConnectionError provides context about connection acquisition failures
type ConnectionError struct {
	ConnectionStr string // Connection string (sensitive parts masked)
	Err           error  // The underlying error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("failed to acquire connection from pool: %v", e.Err)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// SchemaError provides context about Arrow schema creation failures
type SchemaError struct {
	Columns []ColumnInfo // Column metadata that failed to convert
	Err     error        // The underlying error
}

func (e *SchemaError) Error() string {
	return fmt.Sprintf("failed to create Arrow schema from %d columns: %v", len(e.Columns), e.Err)
}

func (e *SchemaError) Unwrap() error {
	return e.Err
}

// maskConnectionString removes sensitive information from connection strings for error reporting
func maskConnectionString(connStr string) string {
	// For now, just return a generic message to avoid leaking credentials
	// In a production implementation, you might want to parse and selectively mask parts
	return "[connection details masked]"
}
