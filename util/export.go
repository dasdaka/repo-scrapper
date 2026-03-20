package util

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
)

// ExportCSV writes a CSV file for every repo in the list by reading pr_report.
// It is a no-op when cfg.CSVExportPath is empty.
// Call this after Aggregate to ensure the report is up to date.
func ExportCSV(ctx context.Context, db *sql.DB, cfg ReportConfig, repos []string, logf LogFunc) error {
	if cfg.CSVExportPath == "" {
		return nil
	}
	for _, repo := range repos {
		path := fmt.Sprintf(cfg.CSVExportPath, repo)
		logf("export: writing %s", path)
		if err := exportReportCSV(ctx, db, cfg, repo); err != nil {
			return fmt.Errorf("export CSV for %s: %w", repo, err)
		}
		logf("export: %s written", path)
	}
	return nil
}

// exportReportCSV reads the pr_report table for the given repo and writes
// a CSV file at the path specified by cfg.CSVExportPath.
// The CSV is purely an export convenience; the database is the source of truth.
func exportReportCSV(ctx context.Context, db *sql.DB, cfg ReportConfig, repo string) error {
	if cfg.CSVExportPath == "" {
		return nil // CSV export not configured; skip silently.
	}

	rows, err := db.QueryContext(ctx, `
		SELECT pr_id, src_repo, src_branch, dest_repo, dest_branch,
		       title, description, state, author, created, updated,
		       file_changed, added, removed, total,
		       activity_type, activity_user, activity_content
		FROM pr_report
		WHERE repo = $1
		ORDER BY pr_id, activity_type, activity_user
	`, repo)
	if err != nil {
		return fmt.Errorf("query pr_report: %w", err)
	}
	defer rows.Close()

	file, err := os.Create(fmt.Sprintf(cfg.CSVExportPath, repo))
	if err != nil {
		return fmt.Errorf("create CSV file: %w", err)
	}
	defer file.Close()

	w := csv.NewWriter(file)
	defer w.Flush()

	header := []string{
		"ID", "SrcRepo", "SrcBranch", "DestRepo", "DestBranch",
		"Title", "Description", "State", "Author", "Created", "Updated",
		"FileChanged", "Added", "Removed", "Total",
		"Type", "User", "Content",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	for rows.Next() {
		var (
			prID                                     int
			srcRepo, srcBranch, destRepo, destBranch string
			title, description, state, author        string
			created, updated                         string
			fileChanged, added, removed, total       int
			actType, actUser, actContent             string
		)
		if err := rows.Scan(
			&prID, &srcRepo, &srcBranch, &destRepo, &destBranch,
			&title, &description, &state, &author, &created, &updated,
			&fileChanged, &added, &removed, &total,
			&actType, &actUser, &actContent,
		); err != nil {
			return err
		}
		if err := w.Write([]string{
			fmt.Sprintf("%d", prID),
			srcRepo, srcBranch, destRepo, destBranch,
			title, description, state, author, created, updated,
			fmt.Sprintf("%d", fileChanged),
			fmt.Sprintf("%d", added),
			fmt.Sprintf("%d", removed),
			fmt.Sprintf("%d", total),
			actType, actUser, actContent,
		}); err != nil {
			return err
		}
	}
	return rows.Err()
}
