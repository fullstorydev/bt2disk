// Command bt2disk syncs the contents of a BigTable instance (typically, a local emulator) to/from the local disk.
package main

import (
	"cloud.google.com/go/bigtable"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/fullstorydev/bt2disk"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"strings"
)

func main() {
	db := flag.String("db", "", "Target sqlite file to save to or restore from")
	project := flag.String("project", "local", "GCP project to connect to")
	instance := flag.String("instance", "local", "BigTable instance to connect to")
	gcp := flag.Bool("gcp", false, "Set to 'true' to connect to real GCP instances (safeguard)")

	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "Usage:  bt2disk [-d DIR] restore|save\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if os.Getenv("BIGTABLE_EMULATOR_HOST") == "" && !*gcp {
		_, _ = fmt.Fprintf(os.Stderr, "bt2disk: BIGTABLE_EMULATOR_HOST must be set OR -gcp=true must be set\n")
		os.Exit(2)
	}

	if *db == "" {
		*db = *instance + ".db"
	}

	args := flag.Args()
	switch len(args) {
	case 0:
		_, _ = fmt.Fprintf(os.Stderr, "bt2disk: must provide an action ('restore' or 'save')\n")
		os.Exit(2)
	case 1:
		// fallthrough
	default:
		_, _ = fmt.Fprintf(os.Stderr, "bt2disk: expected only a single argument\n")
		os.Exit(2)
	}

	adminClient, err := bigtable.NewAdminClient(ctx, *project, *instance)
	if err != nil {
		log.Fatalf("failed to connect to bigtable instance: %s", err)
	}

	btClient, err := bigtable.NewClient(ctx, *project, *instance)
	if err != nil {
		log.Fatalf("failed to connect to bigtable instance: %s", err)
	}

	dbClient, err := sql.Open("sqlite3", *db)
	if err != nil {
		log.Fatalf("failed to open %q: %s", *db, err)
	}

	switch strings.ToLower(args[0]) {
	case "restore":
		if err := bt2disk.Restore(ctx, dbClient, adminClient, btClient); err != nil {
			log.Fatalf("failed to restore: %s", err)
		}
	case "save":
		if err := bt2disk.SaveAll(ctx, dbClient, adminClient, btClient); err != nil {
			log.Fatalf("failed to save: %s", err)
		}
	default:
		_, _ = fmt.Fprintf(os.Stderr, "bt2disk: unrecognized action: %q\n", args[0])
		os.Exit(2)
	}

	if err := dbClient.Close(); err != nil {
		log.Fatalf("failed to close database: %s", err)
	}
}
