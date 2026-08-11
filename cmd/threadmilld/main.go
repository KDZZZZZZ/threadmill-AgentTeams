package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/app"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		writeUsage(stderr)
		return 2
	}

	switch args[0] {
	case "serve":
		serveFlags := flag.NewFlagSet("serve", flag.ContinueOnError)
		serveFlags.SetOutput(stderr)
		fake := serveFlags.Bool("fake", false, "run the canonical in-memory acceptance host")
		httpAddr := serveFlags.String("http-addr", "", "HTTP listen address")
		webDist := serveFlags.String("web-dist", "", "built Web UI directory (fake mode)")
		if err := serveFlags.Parse(args[1:]); err != nil {
			return 2
		}
		if serveFlags.NArg() != 0 {
			fmt.Fprintf(stderr, "unexpected serve argument %q\n", serveFlags.Arg(0))
			return 2
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if *fake {
			if *httpAddr == "" {
				*httpAddr = "127.0.0.1:8080"
			}
			if *webDist == "" {
				*webDist = "web/dist"
			}
			if err := app.ServeFake(ctx, *httpAddr, *webDist); err != nil {
				writeDiagnostic(stdout, config.Diagnostic{OK: false, Code: "serve_failed", Message: err.Error(), Recoverable: true})
				return 1
			}
			return 0
		}
		if *webDist != "" {
			fmt.Fprintln(stderr, "--web-dist requires --fake")
			return 2
		}
		cfg, err := config.Load(nil)
		if err != nil {
			writeDiagnostic(stdout, diagnosticFromError(err))
			return 2
		}
		if *httpAddr != "" {
			cfg.HTTPAddr = *httpAddr
		}
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
	case "bootstrap-operator":
		bootstrapFlags := flag.NewFlagSet("bootstrap-operator", flag.ContinueOnError)
		bootstrapFlags.SetOutput(stderr)
		actor := bootstrapFlags.String("actor", "", "operator principal ID")
		ttl := bootstrapFlags.Duration("ttl", 8*time.Hour, "session lifetime (maximum 24h)")
		if err := bootstrapFlags.Parse(args[1:]); err != nil {
			return 2
		}
		if bootstrapFlags.NArg() != 0 || *actor == "" {
			fmt.Fprintln(stderr, "bootstrap-operator requires --actor and accepts no positional arguments")
			return 2
		}
		cfg, err := config.Load(nil)
		if err != nil {
			writeDiagnostic(stdout, diagnosticFromError(err))
			return 2
		}
		credential, err := app.BootstrapOperator(context.Background(), cfg, kernel.ActorPrincipalID(*actor), *ttl)
		if err != nil {
			writeDiagnostic(stdout, config.Diagnostic{OK: false, Code: "operator_bootstrap_failed", Message: err.Error(), Recoverable: true})
			return 1
		}
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(credential); err != nil {
			fmt.Fprintln(stderr, "write operator bootstrap output failed")
			return 1
		}
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
		fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		writeUsage(stderr)
		return 2
	}
}

func writeUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: threadmilld serve [--fake] [--http-addr addr] [--web-dist dir] | migrate | check | bootstrap-operator --actor id [--ttl 8h]")
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
