package pgarrow

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool wraps pgxpool.Pool and provides Arrow query capabilities
type Pool struct {
	pool *pgxpool.Pool
}

// NewPool creates a new PGArrow pool
func NewPool(ctx context.Context, connString string) (*Pool, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &Pool{pool: pool}, nil
}

// QueryArrow executes a query and returns Arrow record
func (p *Pool) QueryArrow(ctx context.Context, sql string, args ...interface{}) (arrow.Record, error) {
	// TODO: Implement COPY TO BINARY and Arrow conversion
	return nil, nil
}

// Close closes the pool
func (p *Pool) Close() {
	p.pool.Close()
}