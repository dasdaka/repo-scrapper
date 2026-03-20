package util

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// Aggregate reads the raw DB tables for every repo in the list and rebuilds
// pr_report. Run this after ScrapeRaw to refresh the aggregated view.
func Aggregate(ctx context.Context, db *sql.DB, repos []string, logf LogFunc) error {
	for _, repo := range repos {
		logf("[%s] aggregating pr_report", repo)
		data, err := loadReportData(ctx, db, repo)
		if err != nil {
			return fmt.Errorf("load data for %s: %w", repo, err)
		}
		if err := PopulateReportTable(ctx, db, repo, data); err != nil {
			return fmt.Errorf("populate report for %s: %w", repo, err)
		}
		logf("[%s] aggregated %d PRs", repo, len(data))
	}
	return nil
}

// loadReportData reads pull_requests, pr_activity, and pr_diffstat from the DB
// and assembles the structure that PopulateReportTable expects.
func loadReportData(ctx context.Context, db *sql.DB, repo string) (map[int]*PullRequestReportData, error) {
	prs, err := queryPullRequests(ctx, db, repo)
	if err != nil {
		return nil, err
	}
	activities, err := queryActivities(ctx, db, repo)
	if err != nil {
		return nil, err
	}
	diffstats, err := queryDiffStats(ctx, db, repo)
	if err != nil {
		return nil, err
	}
	return mapPrWithOtherData(prs, activities, diffstats), nil
}

func queryPullRequests(ctx context.Context, db *sql.DB, repo string) ([]PullRequestData, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT raw_json FROM pull_requests WHERE repo = $1 ORDER BY pr_id", repo)
	if err != nil {
		return nil, fmt.Errorf("query pull_requests: %w", err)
	}
	defer rows.Close()

	var out []PullRequestData
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var pr PullRequestData
		if err := json.Unmarshal([]byte(raw), &pr); err != nil {
			return nil, fmt.Errorf("unmarshal PR: %w", err)
		}
		out = append(out, pr)
	}
	return out, rows.Err()
}

func queryActivities(ctx context.Context, db *sql.DB, repo string) ([]PullRequestActivityData, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT raw_json FROM pr_activity WHERE repo = $1 ORDER BY pr_id", repo)
	if err != nil {
		return nil, fmt.Errorf("query pr_activity: %w", err)
	}
	defer rows.Close()

	var out []PullRequestActivityData
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var act PullRequestActivityData
		if err := json.Unmarshal([]byte(raw), &act); err != nil {
			return nil, fmt.Errorf("unmarshal activity: %w", err)
		}
		out = append(out, act)
	}
	return out, rows.Err()
}

func queryDiffStats(ctx context.Context, db *sql.DB, repo string) ([]DiffStatActivityData, error) {
	// PullRequestID is a Go-side field not present in raw_json; read it from pr_id column.
	rows, err := db.QueryContext(ctx,
		"SELECT pr_id, raw_json FROM pr_diffstat WHERE repo = $1 ORDER BY pr_id", repo)
	if err != nil {
		return nil, fmt.Errorf("query pr_diffstat: %w", err)
	}
	defer rows.Close()

	var out []DiffStatActivityData
	for rows.Next() {
		var prID int
		var raw string
		if err := rows.Scan(&prID, &raw); err != nil {
			return nil, err
		}
		var ds DiffStatActivityData
		if err := json.Unmarshal([]byte(raw), &ds); err != nil {
			return nil, fmt.Errorf("unmarshal diffstat: %w", err)
		}
		ds.PullRequestID = prID
		out = append(out, ds)
	}
	return out, rows.Err()
}
