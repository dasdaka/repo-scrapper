package util

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// ScrapePipelinesRaw fetches pipeline data from the Bitbucket API for every
// configured repo and stores it in the raw DB tables.
//
// Strategy:
//  1. Fetch pipelines sorted by created_on desc with early-exit at fromDate.
//  2. Filter client-side to [fromDate, toDate].
//  3. Upsert pipelines to DB.
//  4. For each pipeline, fetch its steps via /pipelines/{uuid}/steps.
//     Any step that has a deployment_uuid triggers a fetch of the single
//     deployment via /deployments/{uuid} — but only if that pipeline_uuid is
//     not already cached in the deployments table.
//  5. Filter deployments by production_envs (if configured) before upserting.
func (c *Client) ScrapePipelinesRaw(ctx context.Context, db *sql.DB, fromDate, toDate time.Time) error {
	n := len(c.cfg.RepoList)
	c.logf("[scrape-pipelines] starting: %d repo(s) to process (range: %s)", n, formatDateRange(fromDate, toDate))
	totalStart := time.Now()

	prodEnvNames := c.cfg.ProductionEnvs
	if len(prodEnvNames) > 0 {
		c.logf("[scrape-pipelines] production env filter: %v", prodEnvNames)
	}

	for i, repo := range c.cfg.RepoList {
		c.logf("[scrape-pipelines] [%d/%d] %s: begin", i+1, n, repo)
		repoStart := time.Now()

		// --- 1. Pipelines: sort=-created_on + early-exit, filter client-side ---
		c.logf("[%s] fetching pipelines (range: %s)", repo, formatDateRange(fromDate, toDate))
		t0 := time.Now()
		pipelines, err := c.fetchRepoPipelines(ctx, repo, fromDate, toDate)
		if err != nil {
			return fmt.Errorf("repo %s: fetch pipelines: %w", repo, err)
		}
		c.logf("[%s] %d pipelines fetched in %s", repo, len(pipelines), time.Since(t0).Round(time.Millisecond))

		pipelines = filterPipelines(pipelines, fromDate, toDate)
		c.logf("[%s] %d pipelines after date filter", repo, len(pipelines))

		t0 = time.Now()
		if err := UpsertPipelines(ctx, db, repo, pipelines); err != nil {
			return fmt.Errorf("repo %s: store pipelines: %w", repo, err)
		}
		c.logf("[%s] stored %d pipelines (%s)", repo, len(pipelines), time.Since(t0).Round(time.Millisecond))

		// --- 2. Per-pipeline deployment fetch via steps ---
		var newDeps, cachedDeps, skippedDeps int
		for _, p := range pipelines {
			// Skip API call if deployment already cached in DB.
			cached, err := DeploymentExistsByPipelineUUID(ctx, db, repo, p.UUID)
			if err != nil {
				c.logf("[%s] WARN: cache check failed for pipeline %s: %v", repo, p.UUID, err)
			}
			if cached {
				cachedDeps++
				continue
			}

			// Fetch steps for this pipeline to find deployment UUIDs.
			steps, err := c.fetchPipelineSteps(ctx, repo, p.UUID)
			if err != nil {
				c.logf("[%s] WARN: fetch steps for pipeline %s: %v", repo, p.UUID, err)
				continue
			}

			depSteps := 0
			for _, step := range steps {
				if step.DeploymentUUID == "" {
					continue
				}
				depSteps++
				c.logf("[%s] pipeline %s step %q → deployment %s", repo, p.UUID, step.Name, step.DeploymentUUID)

				dep, err := c.fetchDeployment(ctx, repo, step.DeploymentUUID)
				if err != nil {
					c.logf("[%s] WARN: fetch deployment %s: %v", repo, step.DeploymentUUID, err)
					continue
				}
				c.logf("[%s] deployment %s env=%q pipeline_uuid=%q", repo, dep.UUID, dep.Environment.Name, dep.Pipeline.UUID)

				// Apply production env filter if configured.
				if len(prodEnvNames) > 0 && !matchesEnvName(dep.Environment.Name, prodEnvNames) {
					c.logf("[%s] skipping deployment %s (env %q not in production_envs)", repo, dep.UUID, dep.Environment.Name)
					skippedDeps++
					continue
				}

				if err := UpsertDeployments(ctx, db, repo, []DeploymentData{dep}); err != nil {
					return fmt.Errorf("repo %s: store deployment %s: %w", repo, dep.UUID, err)
				}
				newDeps++
			}
			if depSteps == 0 && len(steps) > 0 {
				c.logf("[%s] pipeline %s has %d steps but none with a deployment_uuid — first step raw: %s",
					repo, p.UUID, len(steps), string(steps[0].RawJSON))
			}
		}
		c.logf("[%s] deployments: %d new, %d cached (skipped API), %d filtered by env",
			repo, newDeps, cachedDeps, skippedDeps)

		c.logf("[scrape-pipelines] [%d/%d] %s: completed in %s",
			i+1, n, repo, time.Since(repoStart).Round(time.Millisecond))
	}

	c.logf("[scrape-pipelines] all %d repo(s) done in %s", n, time.Since(totalStart).Round(time.Second))
	return nil
}

// fetchPipelineByUUID fetches a single pipeline record by its UUID via
// GET /repositories/{workspace}/{repo}/pipelines/{uuid}.
func (c *Client) fetchPipelineByUUID(ctx context.Context, repo, pipelineUUID string) (PipelineData, error) {
	endpoint := fmt.Sprintf(
		"https://api.bitbucket.org/2.0/repositories/%s/%s/pipelines/%s",
		url.PathEscape(c.cfg.Workspace),
		url.PathEscape(repo),
		url.PathEscape(pipelineUUID),
	)
	c.logf("[%s] pipeline URL: %s", repo, endpoint)
	var p PipelineData
	err := c.getJSON(ctx, endpoint, &p)
	return p, err
}

// fetchRepoPipelines retrieves pipeline runs for a single repo sorted by
// created_on descending, stopping pagination once all remaining pipelines
// pre-date fromDate (zero = fetch all pages). The Bitbucket Pipelines API does
// not support q= filtering (BCLOUD-14000), so date filtering is done client-side.
func (c *Client) fetchRepoPipelines(ctx context.Context, repo string, fromDate, toDate time.Time) ([]PipelineData, error) {
	firstURL := fmt.Sprintf(
		"https://api.bitbucket.org/2.0/repositories/%s/%s/pipelines?pagelen=%d&sort=-created_on",
		url.PathEscape(c.cfg.Workspace),
		url.PathEscape(repo),
		defaultPagelen,
	)
	c.logf("[%s] pipeline list URL: %s", repo, firstURL)
	// The Bitbucket Pipelines API does not support q= date filtering (BCLOUD-14000).
	// We use sort=-created_on so pages arrive newest-first, then stop as soon as a
	// pipeline's created_on falls before fromDate — no older pages need to be fetched.
	// The toDate upper bound is applied client-side in filterPipelines afterwards.
	if !fromDate.IsZero() {
		c.logf("[%s] early-exit pagination: stop fetching when created_on < %s (client-side toDate=%s)",
			repo, fromDate.Format("2006-01-02"), toDate.Format("2006-01-02"))
	}
	stop := func(p PipelineData) bool {
		return !fromDate.IsZero() && p.CreatedOn.Before(fromDate)
	}
	return fetchPagesUntil(ctx, c, firstURL, "["+repo+"] pipelines", stop)
}

// fetchPipelineSteps returns all steps for a single pipeline run.
// Steps that deployed to an environment carry a non-empty DeploymentUUID.
// The raw JSON of each step is captured in RawJSON for field-name diagnostics.
func (c *Client) fetchPipelineSteps(ctx context.Context, repo, pipelineUUID string) ([]PipelineStep, error) {
	firstURL := fmt.Sprintf(
		"https://api.bitbucket.org/2.0/repositories/%s/%s/pipelines/%s/steps?pagelen=%d",
		url.PathEscape(c.cfg.Workspace),
		url.PathEscape(repo),
		url.PathEscape(pipelineUUID),
		defaultPagelen,
	)
	// Fetch raw then decode individually so we can capture each step's raw JSON.
	type rawPage struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	var steps []PipelineStep
	pageURL := firstURL
	for pageURL != "" {
		var page rawPage
		if err := c.getJSON(ctx, pageURL, &page); err != nil {
			return nil, err
		}
		for _, raw := range page.Values {
			var s PipelineStep
			if err := json.Unmarshal(raw, &s); err != nil {
				return nil, err
			}
			s.RawJSON = raw
			steps = append(steps, s)
		}
		pageURL = page.Next
	}
	return steps, nil
}

// fetchDeployment fetches a single deployment record by its UUID.
func (c *Client) fetchDeployment(ctx context.Context, repo, deploymentUUID string) (DeploymentData, error) {
	endpoint := fmt.Sprintf(
		"https://api.bitbucket.org/2.0/repositories/%s/%s/deployments/%s",
		url.PathEscape(c.cfg.Workspace),
		url.PathEscape(repo),
		url.PathEscape(deploymentUUID),
	)
	c.logf("[%s] deployment URL: %s", repo, endpoint)
	var d DeploymentData
	err := c.getJSON(ctx, endpoint, &d)
	return d, err
}

// --- filter helpers ---

// filterPipelines returns pipelines whose created_on falls within [fromDate, toDate].
// Zero values mean no bound on that side.
func filterPipelines(pipelines []PipelineData, fromDate, toDate time.Time) []PipelineData {
	out := pipelines[:0]
	for _, p := range pipelines {
		if !fromDate.IsZero() && p.CreatedOn.Before(fromDate) {
			continue
		}
		if !toDate.IsZero() && p.CreatedOn.After(toDate) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// matchesEnvName reports whether envName matches any of the configured names
// (case-insensitive).
func matchesEnvName(envName string, names []string) bool {
	for _, n := range names {
		if strings.EqualFold(envName, n) {
			return true
		}
	}
	return false
}
