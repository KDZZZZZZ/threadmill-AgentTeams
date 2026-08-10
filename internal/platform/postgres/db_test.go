package postgres

import (
	"context"
	"testing"
)

func TestOpenRequiresDatabaseURL(t *testing.T) {
	t.Parallel()

	if _, err := Open(context.Background(), ""); err == nil {
		t.Fatal("Open() error = nil, want database url validation error")
	}
}
