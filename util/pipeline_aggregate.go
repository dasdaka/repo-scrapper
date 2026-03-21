package util

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// AggregatePipelines reads the pipelines and deployments raw tables for every
// repo in the list and rebuilds pipeline_report. Run this after ScrapePipelinesRaw
// to refresh the aggregated view.
func AggregatePipelines(ctx context.Context, db *sql.DB, repos []string, logf LogFunc) error {
	for _, repo := range repos {
		logf("[%s] aggregating pipeline_report", repo)
		reportData, err := loadPipelineReportData(ctx, db, repo)
		if err != nil {
			return fmt.Errorf("pipeline load data for %s: %w", repo, err)
		}
		count, err := populatePipelineReport(ctx, db, repo, reportData)
		if err != nil {
			return fmt.Errorf("pipeline aggregate for %s: %w", repo, err)
		}
		logf("[%s] aggregated %d pipeline rows", repo, count)
	}
	return nil
}

// loadPipelineReportData reads raw pipelines and deployments from the DB and
// performs the LEFT JOIN in Go, returning a slice of PipelineReportData.
func loadPipelineReportData(ctx context.Context, db *sql.DB, repo string) ([]PipelineReportData, error) {
	pipelines, err := queryRawPipelines(ctx, db, repo)
	if err != nil {
		return nil, err
	}
	deployments, err := queryRawDeployments(ctx, db, repo)
	if err != nil {
		return nil, err
	}
	return mapPipelineWithDeployments(pipelines, deployments), nil
}

// queryRawPipelines reads the raw_json column from the pipelines table and
// deserialises each row into a PipelineData.
func queryRawPipelines(ctx context.Context, db *sql.DB, repo string) ([]PipelineData, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT raw_json FROM pipelines WHERE repo = $1 ORDER BY created_on DESC", repo)
	if err != nil {
		return nil, fmt.Errorf("query pipelines: %w", err)
	}
	defer rows.Close()

	var out []PipelineData
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var p PipelineData
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return nil, fmt.Errorf("unmarshal pipeline: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// queryRawDeployments reads the raw_json column from the deployments table and
// deserialises each row into a DeploymentData.
func queryRawDeployments(ctx context.Context, db *sql.DB, repo string) ([]DeploymentData, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT raw_json FROM deployments WHERE repo = $1", repo)
	if err != nil {
		return nil, fmt.Errorf("query deployments: %w", err)
	}
	defer rows.Close()

	var out []DeploymentData
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var d DeploymentData
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			return nil, fmt.Errorf("unmarshal deployment: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// mapPipelineWithDeployments implements LEFT JOIN semantics in Go:
// each pipeline gets its matching deployments keyed by pipeline UUID.
// Pipelines with no matching deployments appear once with an empty deployments slice.
func mapPipelineWithDeployments(pipelines []PipelineData, deployments []DeploymentData) []PipelineReportData {
	// Build a map from pipeline UUID to its deployments.
	depMap := make(map[string][]DeploymentData, len(deployments))
	for _, d := range deployments {
		depMap[d.Pipeline.UUID] = append(depMap[d.Pipeline.UUID], d)
	}

	out := make([]PipelineReportData, 0, len(pipelines))
	for _, p := range pipelines {
		deps := depMap[p.UUID] // nil if no deployments; that's fine
		if deps == nil {
			deps = []DeploymentData{}
		}
		out = append(out, PipelineReportData{
			pipeline:    p,
			deployments: deps,
		})
	}
	return out
}

// populatePipelineReport rebuilds pipeline_report for a single repo using the
// pre-joined PipelineReportData slice. Each (pipeline, deployment) pair becomes
// one row; pipelines with no deployments get one row with empty environment fields.
//
// Returns the number of rows inserted.
func populatePipelineReport(ctx context.Context, db *sql.DB, repo string, reportData []PipelineReportData) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM pipeline_report WHERE repo = $1", repo); err != nil {
		return 0, fmt.Errorf("delete pipeline_report for %s: %w", repo, err)
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pipeline_report
		(pipeline_uuid, repo, build_number, run_number,
		 creator, target_ref_type, target_ref_name, trigger_name,
		 state_name, result_name,
		 environment_uuid, environment_name, deployment_state, deployment_status,
		 created_on, completed_on, duration_seconds)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
	`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	count := 0
	for _, r := range reportData {
		p := r.pipeline
		createdOnStr := p.CreatedOn.Format(time.RFC3339)
		completedOnStr := ""
		if p.CompletedOn != nil {
			completedOnStr = p.CompletedOn.Format(time.RFC3339)
		}

		if len(r.deployments) == 0 {
			// Pipeline with no deployment: insert one row with empty env fields.
			durationSeconds := computeDuration(createdOnStr, completedOnStr, p.BuildSecondsUsed)
			if _, err := stmt.ExecContext(ctx,
				p.UUID, repo, p.BuildNumber, p.RunNumber,
				p.Creator.DisplayName, p.Target.RefType, p.Target.RefName, p.TriggerName(),
				p.State.Name, p.State.Result.Name,
				"", "", "", "",
				createdOnStr, completedOnStr, durationSeconds,
			); err != nil {
				return count, fmt.Errorf("insert pipeline_report for pipeline %s: %w", p.UUID, err)
			}
			count++
			continue
		}

		// One row per deployment.
		for _, d := range r.deployments {
			durationSeconds := computeDuration(createdOnStr, completedOnStr, p.BuildSecondsUsed)
			if _, err := stmt.ExecContext(ctx,
				p.UUID, repo, p.BuildNumber, p.RunNumber,
				p.Creator.DisplayName, p.Target.RefType, p.Target.RefName, p.TriggerName(),
				p.State.Name, p.State.Result.Name,
				d.Environment.UUID, d.Environment.Name, d.State.Name, d.State.Status.Name,
				createdOnStr, completedOnStr, durationSeconds,
			); err != nil {
				return count, fmt.Errorf("insert pipeline_report for pipeline %s deployment %s: %w", p.UUID, d.UUID, err)
			}
			count++
		}
	}

	return count, tx.Commit()
}

// computeDuration returns the wall-clock duration in seconds between createdOn
// and completedOn. Falls back to buildSecondsUsed when the timestamps cannot be
// parsed or the result would be non-positive.
func computeDuration(createdOnStr, completedOnStr string, buildSecondsUsed int) int {
	if createdOnStr != "" && completedOnStr != "" {
		created, err1 := time.Parse(time.RFC3339, createdOnStr)
		completed, err2 := time.Parse(time.RFC3339, completedOnStr)
		if err1 == nil && err2 == nil {
			if d := int(completed.Sub(created).Seconds()); d > 0 {
				return d
			}
		}
	}
	return buildSecondsUsed
}
