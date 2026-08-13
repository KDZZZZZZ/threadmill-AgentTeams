package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

type Tx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type transaction interface {
	Tx
	Commit() error
	Rollback() error
}

type beginner interface {
	BeginTx(context.Context, *sql.TxOptions) (transaction, error)
}

type sqlDB struct {
	db *sql.DB
}

func (s sqlDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (transaction, error) {
	return s.db.BeginTx(ctx, opts)
}

type TxRunner struct {
	db beginner
}

func NewTxRunner(db beginner) *TxRunner {
	return &TxRunner{db: db}
}

func NewSQLTxRunner(db *sql.DB) *TxRunner {
	return NewTxRunner(sqlDB{db: db})
}

func (r *TxRunner) WithinTx(ctx context.Context, opts *sql.TxOptions, fn func(context.Context, Tx) error) error {
	tx, err := r.db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	if err := fn(ctx, tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("rollback after callback error %w: %v", err, rollbackErr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
