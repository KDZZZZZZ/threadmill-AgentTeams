package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestWithinTxRollsBackWhenCallbackFails(t *testing.T) {
	t.Parallel()

	db := &fakeBeginner{}
	runner := NewTxRunner(db)
	errFailure := errors.New("business write failed")

	err := runner.WithinTx(context.Background(), nil, func(context.Context, Tx) error {
		return errFailure
	})
	if !errors.Is(err, errFailure) {
		t.Fatalf("WithinTx error = %v, want %v", err, errFailure)
	}
	if !db.tx.rolledBack {
		t.Fatal("transaction was not rolled back")
	}
	if db.tx.committed {
		t.Fatal("transaction was committed after callback failure")
	}
}

func TestWithinTxCommitsOnSuccess(t *testing.T) {
	t.Parallel()

	db := &fakeBeginner{}
	runner := NewTxRunner(db)

	if err := runner.WithinTx(context.Background(), nil, func(context.Context, Tx) error {
		return nil
	}); err != nil {
		t.Fatalf("WithinTx returned error: %v", err)
	}
	if !db.tx.committed {
		t.Fatal("transaction was not committed")
	}
	if db.tx.rolledBack {
		t.Fatal("transaction was rolled back after success")
	}
}

type fakeBeginner struct {
	tx fakeTx
}

func (f *fakeBeginner) BeginTx(context.Context, *sql.TxOptions) (transaction, error) {
	return &f.tx, nil
}

type fakeTx struct {
	committed  bool
	rolledBack bool
}

func (f *fakeTx) Commit() error {
	f.committed = true
	return nil
}

func (f *fakeTx) Rollback() error {
	f.rolledBack = true
	return nil
}

func (f *fakeTx) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, nil
}

func (f *fakeTx) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}

func (f *fakeTx) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return nil
}
