package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/app"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: threadmilld serve|migrate|check")
		return 2
	}

	switch args[0] {
	case "serve":
		cfg, err := config.Load(nil)
		if err != nil {
			writeDiagnostic(stdout, diagnosticFromError(err))
			return 2
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := app.Serve(ctx, cfg); err != nil {
			writeDiagnostic(stdout, config.Diagnostic{OK: false, Code: "serve_failed", Message: err.Error(), Recoverable: true})
			return 1
		}
		return 0
	case "migrate":
		cfg, err := config.Load(nil)
		if err != nil {
			writeDiagnostic(stdout, diagnosticFromError(err))
			return 2
		}
		if err := app.Migrate(context.Background(), cfg); err != nil {
			writeDiagnostic(stdout, config.Diagnostic{OK: false, Code: "migration_failed", Message: err.Error(), Recoverable: true})
			return 1
		}
		writeDiagnostic(stdout, config.Diagnostic{OK: true, Code: "ok", Message: "migrations are up to date", Recoverable: true})
		return 0
	case "check":
		cfg, err := config.Load(nil)
		if err != nil {
			writeDiagnostic(stdout, diagnosticFromError(err))
			return 2
		}
		writeDiagnostic(stdout, config.Check(cfg))
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\nusage: threadmilld serve|migrate|check\n", args[0])
		return 2
	}
}

func diagnosticFromError(err error) config.Diagnostic {
	if diag, ok := config.AsDiagnostic(err); ok {
		return diag
	}
	return config.Diagnostic{OK: false, Code: "internal_error", Message: err.Error(), Recoverable: false}
}

func writeDiagnostic(w io.Writer, diag config.Diagnostic) {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(diag)
}
