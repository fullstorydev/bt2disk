// Command bt2disk syncs the contents of a BigTable instance (typically, a local emulator) to/from the local disk.
package bt2disk

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	_ "github.com/mattn/go-sqlite3"
)

func filterErrors(errs []error) []error {
	var out []error
	for _, err := range errs {
		if err != nil {
			out = append(out, err)
		}
	}
	return out
}

func Restore(ctx context.Context, db *sql.DB, adminClient *bigtable.AdminClient, btClient *bigtable.Client) error {
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

		if err := RestoreTable(ctx, table, db, btClient); err != nil {
			return fmt.Errorf("failed to restore table %q: %s", table, err)
		}
	}
	return nil
}

func RestoreTable(ctx context.Context, table string, db *sql.DB, btClient *bigtable.Client) error {
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

func SaveAll(ctx context.Context, db *sql.DB, adminClient *bigtable.AdminClient, btClient *bigtable.Client) error {
	tables, err := adminClient.Tables(ctx)
	if err != nil {
		return fmt.Errorf("failed to list BT tables: %s", err)
	}

	sort.Strings(tables)

	for _, table := range tables {
		if err := SaveTable(ctx, table, db, btClient); err != nil {
			return fmt.Errorf("failed to save table %q: %s", table, err)
		}
	}

	return nil
}

func SaveTable(ctx context.Context, table string, db *sql.DB, btClient *bigtable.Client) error {
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
