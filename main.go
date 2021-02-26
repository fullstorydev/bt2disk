// Command bt2disk syncs the contents of a BigTable instance (typically, a local emulator) to/from the local disk.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	_ "github.com/mattn/go-sqlite3"
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

	ctx := context.Background()

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
		if err := restore(ctx, dbClient, adminClient, btClient); err != nil {
			log.Fatalf("failed to restore: %s", err)
		}
	case "save":
		if err := saveAll(ctx, dbClient, adminClient, btClient); err != nil {
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

func filterErrors(errs []error) []error {
	var out []error
	for _, err := range errs {
		if err != nil {
			out = append(out, err)
		}
	}
	return out
}

func restore(ctx context.Context, db *sql.DB, adminClient *bigtable.AdminClient, btClient *bigtable.Client) error {
	tables, err := adminClient.Tables(ctx)
	if err != nil {
		return fmt.Errorf("failed to list BT tables: %s", err)
	}

	sort.Strings(tables)

	for _, table := range tables {
		if err := adminClient.DropAllRows(ctx, table); err != nil {
			return fmt.Errorf("failed to delete table %q", table)
		}
	}

	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return fmt.Errorf("failed to query sqlite for list of tables")
	}

	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return fmt.Errorf("failed to scan table-list results: %s", err)
		}

		if err := restoreTable(ctx, table, db, btClient); err != nil {
			return fmt.Errorf("failed to restore table %q: %s", table, err)
		}
	}
	return nil
}

func restoreTable(ctx context.Context, table string, db *sql.DB, btClient *bigtable.Client) error {
	log.Printf("restoring %q table...", table)

	tbl := btClient.Open(table)

	rows, err := db.Query(`SELECT key, column_family, column, value, timestamp, chk FROM "` + table + `"`)
	if err != nil {
		return fmt.Errorf("failed to query table contents: %s", err)
	}

	var c int
	var keys []string
	var muts []*bigtable.Mutation

	for rows.Next() {
		var key, cf, col string
		var value []byte
		var ts int64
		var chk uint32
		if err := rows.Scan(&key, &cf, &col, &value, &ts, &chk); err != nil {
			return fmt.Errorf("failed to scan table-list results: %s", err)
		}

		timestamp := time.Unix(0, ts)

		hasher := fnv.New32a()
		_, _ = fmt.Fprintf(hasher, key)
		_, _ = fmt.Fprintf(hasher, cf)
		_, _ = fmt.Fprintf(hasher, col)
		_, _ = hasher.Write(value)
		_, _ = fmt.Fprintf(hasher, timestamp.Format(time.RFC3339Nano))

		if computed := hasher.Sum32(); chk != computed {
			return fmt.Errorf("integrity check failed, db.chk=%d, computed hash = %d", chk, computed)
		}

		m := bigtable.NewMutation()
		m.Set(cf, col, bigtable.Time(timestamp), value)

		c++
		keys = append(keys, key)
		muts = append(muts, m)

		// nothing magic about 100, tune as necessary - we just need some limit so that grpc payloads don't get too huge
		if len(muts) == 100 {
			if errs, err := tbl.ApplyBulk(ctx, keys, muts); err != nil {
				return fmt.Errorf("failed to write to bigtable: %s", err)
			} else if errs = filterErrors(errs); len(errs) > 0 {
				return fmt.Errorf("failed to write to bigtable, %d errors, first: %s", len(errs), errs[0])
			}

			keys = nil
			muts = nil
		}
	}

	if len(muts) > 0 {
		if errs, err := tbl.ApplyBulk(ctx, keys, muts); err != nil {
			return fmt.Errorf("failed to write to bigtable: %s", err)
		} else if errs = filterErrors(errs); len(errs) > 0 {
			return fmt.Errorf("failed to write to bigtable, %d errors, first: %s", len(errs), errs[0])
		}
	}

	log.Printf("restored %d rows for %s", c, table)
	return nil
}

func saveAll(ctx context.Context, db *sql.DB, adminClient *bigtable.AdminClient, btClient *bigtable.Client) error {
	tables, err := adminClient.Tables(ctx)
	if err != nil {
		return fmt.Errorf("failed to list BT tables: %s", err)
	}

	sort.Strings(tables)

	for _, table := range tables {
		if err := saveTable(ctx, table, db, btClient); err != nil {
			return fmt.Errorf("failed to save table %q: %s", table, err)
		}
	}

	return nil
}

func saveTable(ctx context.Context, table string, db *sql.DB, btClient *bigtable.Client) error {
	log.Printf("saving %q table...", table)
	stmt, err := db.Prepare(`DROP TABLE IF EXISTS "` + table + `"`)
	if err != nil {
		return fmt.Errorf("failed to prepare DROP TABLE: %s", err)
	}
	if _, err := stmt.Exec(); err != nil {
		return fmt.Errorf("failed to execute DROP TABLE: %s", err)
	}

	stmt, err = db.Prepare(`CREATE TABLE IF NOT EXISTS "` + table + `" (key TEXT, column_family TEXT, column TEXT, value BLOB, timestamp INTEGER, chk INTEGER)`)
	if err != nil {
		return fmt.Errorf("failed to prepare CREATE TABLE: %s", err)
	}
	if _, err := stmt.Exec(); err != nil {
		return fmt.Errorf("failed to execute CREATE TABLE: %s", err)
	}

	insertStmt, err := db.Prepare(`INSERT INTO "` + table + `" VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare INSERT: %s", err)
	}

	var c int
	var rowErr error
	tbl := btClient.Open(table)
	err = tbl.ReadRows(ctx, bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
		for cf, items := range row {
			for _, item := range items {
				// tricky!  the item.Column that we get back is prefixed with the column family
				col := strings.TrimPrefix(item.Column, cf+":")

				hasher := fnv.New32a()
				_, _ = fmt.Fprintf(hasher, item.Row)
				_, _ = fmt.Fprintf(hasher, cf)
				_, _ = fmt.Fprintf(hasher, col)
				_, _ = hasher.Write(item.Value)
				_, _ = fmt.Fprintf(hasher, item.Timestamp.Time().Format(time.RFC3339Nano))

				if _, err := insertStmt.Exec(item.Row, cf, col, item.Value, item.Timestamp.Time().UnixNano(), hasher.Sum32()); err != nil {
					rowErr = fmt.Errorf("failed to save row %s: %s", row.Key(), err)
					return false
				}
				c++
			}
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("failure while iterating rows: %s", err)
	}
	if rowErr != nil {
		return rowErr
	}

	log.Printf("saved %d rows for %s", c, table)
	return nil
}
