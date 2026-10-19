package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/config"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/sync"
)

func newCLIDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// RunQuarantine dispatches quarantine subcommands: list, inspect, and requeue.
// Returns 0 on success and nonzero on error.
func RunQuarantine(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printQuarantineUsage(stderr)
		return 1
	}

	subcmd := args[0]
	subArgs := args[1:]

	switch subcmd {
	case "list":
		return runQuarantineList(subArgs, stdout, stderr)
	case "inspect":
		return runQuarantineInspect(subArgs, stdout, stderr)
	case "requeue":
		return runQuarantineRequeue(subArgs, stdout, stderr)
	case "-h", "--help", "help":
		printQuarantineUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "Error: unknown quarantine command %q\n\n", subcmd)
		printQuarantineUsage(stderr)
		return 1
	}
}

func printQuarantineUsage(w io.Writer) {
	fmt.Fprintf(w, "AegisEdge Quarantine Management CLI\n\n")
	fmt.Fprintf(w, "Usage:\n")
	fmt.Fprintf(w, "  aegisedge-agent quarantine <command> [arguments] [flags]\n\n")
	fmt.Fprintf(w, "Commands:\n")
	fmt.Fprintf(w, "  list     List quarantined telemetry batches\n")
	fmt.Fprintf(w, "  inspect  Inspect metadata and payload of a quarantined batch\n")
	fmt.Fprintf(w, "  requeue  Requeue a quarantined batch for re-synchronization\n\n")
	fmt.Fprintf(w, "Flags:\n")
	fmt.Fprintf(w, "  --help, -h   Show help for quarantine commands\n")
}

func resolveDatabasePath(explicitPath string) string {
	if explicitPath != "" {
		return explicitPath
	}
	cfg := config.LoadFromEnv()
	if cfg.DatabasePath != "" {
		return cfg.DatabasePath
	}
	return config.DefaultDatabasePath
}

// parseFlagsAndPositional allows flags and positional arguments in any order.
func parseFlagsAndPositional(args []string, fs *flag.FlagSet) ([]string, error) {
	var flagArgs []string
	var positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			flagArgs = append(flagArgs, arg)
			if !strings.Contains(arg, "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				flagName := strings.TrimLeft(arg, "-")
				f := fs.Lookup(flagName)
				if f != nil {
					if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); !ok || !bf.IsBoolFlag() {
						i++
						flagArgs = append(flagArgs, args[i])
					}
				}
			}
		} else {
			positional = append(positional, arg)
		}
	}
	if err := fs.Parse(flagArgs); err != nil {
		return nil, err
	}
	return positional, nil
}

func runQuarantineList(args []string, stdout, stderr io.Writer) int {
	defaultDB := resolveDatabasePath("")

	fs := flag.NewFlagSet("quarantine list", flag.ContinueOnError)
	fs.SetOutput(stderr)

	dbPath := fs.String("db-path", defaultDB, "Path to local SQLite database")
	limit := fs.Int("limit", 50, "Maximum number of failed batches to list (1..1000)")
	asJSON := fs.Bool("json", false, "Output results as formatted JSON")

	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: aegisedge-agent quarantine list [--db-path <path>] [--limit <n>] [--json]\n\n")
		fmt.Fprintf(stderr, "Flags:\n")
		fs.PrintDefaults()
	}

	if _, err := parseFlagsAndPositional(args, fs); err != nil {
		return 1
	}

	if *limit < 1 || *limit > 1000 {
		fmt.Fprintf(stderr, "Error: limit must be between 1 and 1000 (got %d)\n", *limit)
		return 1
	}

	store, err := storage.OpenSQLite(*dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "Error: failed to open database at %q: %v\n", *dbPath, err)
		return 1
	}
	defer store.Close()

	ctx := context.Background()
	batches, err := store.GetFailedBatches(ctx, *limit)
	if err != nil {
		fmt.Fprintf(stderr, "Error: failed to query quarantined batches: %v\n", err)
		return 1
	}

	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(batches); err != nil {
			fmt.Fprintf(stderr, "Error: failed to serialize JSON output: %v\n", err)
			return 1
		}
		return 0
	}

	if len(batches) == 0 {
		fmt.Fprintf(stdout, "No quarantined batches found.\n")
		return 0
	}

	w := tabwriter.NewWriter(stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "BATCH ID\tNODE ID\tSEQ\tATTEMPTS\tFAILED AT\tREASON\n")
	for _, b := range batches {
		failedAtStr := "unknown"
		if b.FailedAt != nil {
			failedAtStr = b.FailedAt.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\n",
			b.BatchID,
			b.NodeID,
			b.SequenceNumber,
			b.Attempt,
			failedAtStr,
			b.ErrorReason,
		)
	}
	_ = w.Flush()
	fmt.Fprintf(stdout, "\nTotal quarantined: %d\n", len(batches))
	return 0
}

func runQuarantineInspect(args []string, stdout, stderr io.Writer) int {
	defaultDB := resolveDatabasePath("")

	fs := flag.NewFlagSet("quarantine inspect", flag.ContinueOnError)
	fs.SetOutput(stderr)

	dbPath := fs.String("db-path", defaultDB, "Path to local SQLite database")
	asJSON := fs.Bool("json", false, "Output results as formatted JSON")

	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: aegisedge-agent quarantine inspect <batch-id> [--db-path <path>] [--json]\n\n")
		fmt.Fprintf(stderr, "Flags:\n")
		fs.PrintDefaults()
	}

	positional, err := parseFlagsAndPositional(args, fs)
	if err != nil {
		return 1
	}

	if len(positional) == 0 || strings.TrimSpace(positional[0]) == "" {
		fmt.Fprintf(stderr, "Error: <batch-id> is required for inspect\n\n")
		fs.Usage()
		return 1
	}
	batchID := strings.TrimSpace(positional[0])

	store, err := storage.OpenSQLite(*dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "Error: failed to open database at %q: %v\n", *dbPath, err)
		return 1
	}
	defer store.Close()

	ctx := context.Background()
	rec, err := store.GetBatch(ctx, batchID)
	if err != nil {
		if errors.Is(err, storage.ErrBatchNotFound) {
			fmt.Fprintf(stderr, "Error: batch %q not found\n", batchID)
			return 1
		}
		fmt.Fprintf(stderr, "Error: failed to retrieve batch %q: %v\n", batchID, err)
		return 1
	}

	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rec); err != nil {
			fmt.Fprintf(stderr, "Error: failed to serialize JSON output: %v\n", err)
			return 1
		}
		return 0
	}

	failedAtStr := "none"
	if rec.FailedAt != nil {
		failedAtStr = rec.FailedAt.UTC().Format(time.RFC3339)
	}
	reasonStr := "none"
	if rec.ErrorReason != "" {
		reasonStr = rec.ErrorReason
	}

	fmt.Fprintf(stdout, "Batch ID:          %s\n", rec.BatchID)
	fmt.Fprintf(stdout, "Node ID:           %s\n", rec.NodeID)
	fmt.Fprintf(stdout, "Sequence Number:   %d\n", rec.SequenceNumber)
	fmt.Fprintf(stdout, "Status:            %s\n", rec.SyncStatus)
	fmt.Fprintf(stdout, "Attempt Count:     %d\n", rec.Attempt)
	fmt.Fprintf(stdout, "Collected At:      %s\n", rec.CollectedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(stdout, "Failed At:         %s\n", failedAtStr)
	fmt.Fprintf(stdout, "Failure Reason:    %s\n", reasonStr)
	fmt.Fprintf(stdout, "Metrics Count:     %d samples\n", len(rec.Payload.Metrics))
	for _, m := range rec.Payload.Metrics {
		fmt.Fprintf(stdout, "  - %s: %.2f %s (at %s)\n",
			m.Name, m.Value, m.Unit, m.Timestamp.UTC().Format(time.RFC3339))
	}

	return 0
}

func runQuarantineRequeue(args []string, stdout, stderr io.Writer) int {
	defaultDB := resolveDatabasePath("")

	fs := flag.NewFlagSet("quarantine requeue", flag.ContinueOnError)
	fs.SetOutput(stderr)

	dbPath := fs.String("db-path", defaultDB, "Path to local SQLite database")

	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: aegisedge-agent quarantine requeue <batch-id> [--db-path <path>]\n\n")
		fmt.Fprintf(stderr, "Flags:\n")
		fs.PrintDefaults()
	}

	positional, err := parseFlagsAndPositional(args, fs)
	if err != nil {
		return 1
	}

	if len(positional) == 0 || strings.TrimSpace(positional[0]) == "" {
		fmt.Fprintf(stderr, "Error: <batch-id> is required for requeue\n\n")
		fs.Usage()
		return 1
	}
	batchID := strings.TrimSpace(positional[0])

	store, err := storage.OpenSQLite(*dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "Error: failed to open database at %q: %v\n", *dbPath, err)
		return 1
	}
	defer store.Close()

	ctx := context.Background()

	// Requeue via Syncer with AuditStore wired to store
	syncer := sync.NewSyncer(store, nil, newCLIDiscardLogger(), sync.WithAuditStore(store))
	if err := syncer.RequeueBatch(ctx, batchID); err != nil {
		if errors.Is(err, storage.ErrBatchNotFound) {
			fmt.Fprintf(stderr, "Error: batch %q not found\n", batchID)
			return 1
		}
		if errors.Is(err, storage.ErrInvalidBatchTransition) {
			fmt.Fprintf(stderr, "Error: cannot requeue batch %q: %v\n", batchID, err)
			return 1
		}
		fmt.Fprintf(stderr, "Error: failed to requeue batch %q: %v\n", batchID, err)
		return 1
	}

	fmt.Fprintf(stdout, "Batch %q successfully requeued to PENDING (attempts reset to 0).\n", batchID)
	return 0
}
