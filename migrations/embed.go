package migrations

import "embed"

const Dir = "."

//go:embed *.sql
var FS embed.FS
