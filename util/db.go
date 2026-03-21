package util

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

const pgDriver = "postgres"

// OpenDB opens a PostgreSQL database using the given connection string (DSN).
// Example DSN: "postgres://user:password@localhost:5432/repo_scrapper?sslmode=disable"
func OpenDB(dsn string) (*sql.DB, error) {
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/repo_scrapper?sslmode=disable"
	}
	db, err := sql.Open(pgDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return db, nil
}

// CreateSchema creates all tables if they do not already exist.
func CreateSchema(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		// Raw pull request metadata from the PR list endpoint.
		`CREATE TABLE IF NOT EXISTS pull_requests (
			pr_id               BIGINT  NOT NULL,
			repo                TEXT    NOT NULL,
			type                TEXT,
			title               TEXT,
			description         TEXT,
			state               TEXT,
			comment_count       INTEGER DEFAULT 0,
			task_count          INTEGER DEFAULT 0,
			author_display_name TEXT,
			author_uuid         TEXT,
			author_account_id   TEXT,
			author_nickname     TEXT,
			src_repo            TEXT,
			src_branch          TEXT,
			dest_repo           TEXT,
			dest_branch         TEXT,
			created_on          TEXT,
			updated_on          TEXT,
			reviewers           TEXT,
			participants        TEXT,
			raw_json            TEXT NOT NULL,
			PRIMARY KEY (pr_id, repo)
		)`,
		// Raw commits from links.commits.
		`CREATE TABLE IF NOT EXISTS pr_commits (
			id                  BIGSERIAL PRIMARY KEY,
			pr_id               BIGINT NOT NULL,
			repo                TEXT   NOT NULL,
			hash                TEXT,
			message             TEXT,
			date                TEXT,
			author_display_name TEXT,
			author_uuid         TEXT,
			author_account_id   TEXT,
			raw_json            TEXT NOT NULL
		)`,
		// Raw activity events from links.activity.
		`CREATE TABLE IF NOT EXISTS pr_activity (
			id                  BIGSERIAL PRIMARY KEY,
			pr_id               BIGINT NOT NULL,
			repo                TEXT   NOT NULL,
			activity_type       TEXT,
			user_display_name   TEXT,
			user_uuid           TEXT,
			user_account_id     TEXT,
			comment_id          INTEGER DEFAULT 0,
			comment_content_raw TEXT,
			comment_created_on  TEXT,
			comment_updated_on  TEXT,
			approval_date       TEXT,
			raw_json            TEXT NOT NULL
		)`,
		// Raw diffstat entries from links.diffstat.
		`CREATE TABLE IF NOT EXISTS pr_diffstat (
			id            BIGSERIAL PRIMARY KEY,
			pr_id         BIGINT NOT NULL,
			repo          TEXT   NOT NULL,
			file_type     TEXT,
			lines_added   INTEGER DEFAULT 0,
			lines_removed INTEGER DEFAULT 0,
			status        TEXT,
			old_path      TEXT,
			new_path      TEXT,
			raw_json      TEXT NOT NULL
		)`,
		// Raw comments from links.comments.
		`CREATE TABLE IF NOT EXISTS pr_comments (
			id                  BIGSERIAL PRIMARY KEY,
			pr_id               BIGINT NOT NULL,
			repo                TEXT   NOT NULL,
			comment_id          INTEGER DEFAULT 0,
			content_raw         TEXT,
			inline_path         TEXT,
			inline_from         INTEGER DEFAULT 0,
			inline_to           INTEGER DEFAULT 0,
			parent_id           INTEGER DEFAULT 0,
			user_display_name   TEXT,
			user_uuid           TEXT,
			deleted             BOOLEAN DEFAULT FALSE,
			created_on          TEXT,
			updated_on          TEXT,
			raw_json            TEXT NOT NULL
		)`,
		// Raw build/CI statuses from links.statuses.
		`CREATE TABLE IF NOT EXISTS pr_statuses (
			id            BIGSERIAL PRIMARY KEY,
			pr_id         BIGINT NOT NULL,
			repo          TEXT   NOT NULL,
			key           TEXT,
			name          TEXT,
			state         TEXT,
			url           TEXT,
			description   TEXT,
			created_on    TEXT,
			updated_on    TEXT,
			raw_json      TEXT NOT NULL
		)`,
		// Raw Bitbucket pipeline runs scraped from /pipelines.
		`CREATE TABLE IF NOT EXISTS pipelines (
			pipeline_uuid      TEXT NOT NULL,
			repo               TEXT NOT NULL,
			build_number       INTEGER DEFAULT 0,
			run_number         INTEGER DEFAULT 0,
			creator_name       TEXT    DEFAULT '',
			creator_uuid       TEXT    DEFAULT '',
			creator_account_id TEXT    DEFAULT '',
			target_ref_type    TEXT    DEFAULT '',
			target_ref_name    TEXT    DEFAULT '',
			trigger_name       TEXT    DEFAULT '',
			state_name         TEXT    DEFAULT '',
			result_name        TEXT    DEFAULT '',
			created_on         TEXT    DEFAULT '',
			completed_on       TEXT    DEFAULT '',
			build_seconds_used INTEGER DEFAULT 0,
			raw_json           TEXT NOT NULL,
			PRIMARY KEY (pipeline_uuid, repo)
		)`,
		// Raw Bitbucket deployment records scraped from /deployments.
		`CREATE TABLE IF NOT EXISTS deployments (
			deployment_uuid  TEXT NOT NULL,
			repo             TEXT NOT NULL,
			pipeline_uuid    TEXT DEFAULT '',
			environment_uuid TEXT DEFAULT '',
			environment_name TEXT DEFAULT '',
			state_name       TEXT DEFAULT '',
			status_name      TEXT DEFAULT '',
			release_name     TEXT DEFAULT '',
			created_on       TEXT DEFAULT '',
			completed_on     TEXT DEFAULT '',
			raw_json         TEXT NOT NULL,
			PRIMARY KEY (deployment_uuid, repo)
		)`,
		// Aggregated pipeline report: one row per (pipeline, deployment environment).
		// Pipelines with no deployment records appear with empty environment fields.
		`CREATE TABLE IF NOT EXISTS pipeline_report (
			id                BIGSERIAL PRIMARY KEY,
			pipeline_uuid     TEXT    NOT NULL,
			repo              TEXT    NOT NULL,
			build_number      INTEGER DEFAULT 0,
			run_number        INTEGER DEFAULT 0,
			creator           TEXT    DEFAULT '',
			target_ref_type   TEXT    DEFAULT '',
			target_ref_name   TEXT    DEFAULT '',
			trigger_name      TEXT    DEFAULT '',
			state_name        TEXT    DEFAULT '',
			result_name       TEXT    DEFAULT '',
			environment_uuid  TEXT    DEFAULT '',
			environment_name  TEXT    DEFAULT '',
			deployment_state  TEXT    DEFAULT '',
			deployment_status TEXT    DEFAULT '',
			created_on        TEXT    DEFAULT '',
			completed_on      TEXT    DEFAULT '',
			duration_seconds  INTEGER DEFAULT 0
		)`,
		// Aggregated report: one row per non-update activity event per PR.
		`CREATE TABLE IF NOT EXISTS pr_report (
			id               BIGSERIAL PRIMARY KEY,
			pr_id            BIGINT NOT NULL,
			repo             TEXT   NOT NULL,
			src_repo         TEXT,
			src_branch       TEXT,
			dest_repo        TEXT,
			dest_branch      TEXT,
			title            TEXT,
			description      TEXT,
			state            TEXT,
			author           TEXT,
			created          TEXT,
			updated          TEXT,
			file_changed     INTEGER DEFAULT 0,
			added            INTEGER DEFAULT 0,
			removed          INTEGER DEFAULT 0,
			total            INTEGER DEFAULT 0,
			activity_type     TEXT,
			activity_user     TEXT,
			activity_content  TEXT,
			pipeline_uuid     TEXT DEFAULT '',
			environment_name  TEXT DEFAULT '',
			deployment_state  TEXT DEFAULT '',
			deployment_status TEXT DEFAULT ''
		)`,
		// Junction table linking pull requests to their associated pipeline runs.
		`CREATE TABLE IF NOT EXISTS pr_pipelines (
			pr_id         BIGINT NOT NULL,
			repo          TEXT   NOT NULL,
			pipeline_uuid TEXT   NOT NULL,
			PRIMARY KEY (pr_id, repo, pipeline_uuid)
		)`,
		// Idempotent migrations: add new columns to pr_report for databases
		// created before this schema version.
		`ALTER TABLE pr_report ADD COLUMN IF NOT EXISTS pipeline_uuid     TEXT DEFAULT ''`,
		`ALTER TABLE pr_report ADD COLUMN IF NOT EXISTS environment_name  TEXT DEFAULT ''`,
		`ALTER TABLE pr_report ADD COLUMN IF NOT EXISTS deployment_state  TEXT DEFAULT ''`,
		`ALTER TABLE pr_report ADD COLUMN IF NOT EXISTS deployment_status TEXT DEFAULT ''`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}
	return nil
}

// UpsertPipelines upserts pipeline rows for the given repo. Existing rows are
// updated so that re-running the scraper reflects the latest pipeline state
// (e.g. a pipeline that was IN_PROGRESS is now COMPLETED).
func UpsertPipelines(ctx context.Context, db *sql.DB, repo string, pipelines []PipelineData) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pipelines
		(pipeline_uuid, repo, build_number, run_number,
		 creator_name, creator_uuid, creator_account_id,
		 target_ref_type, target_ref_name, trigger_name,
		 state_name, result_name,
		 created_on, completed_on, build_seconds_used, raw_json)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		ON CONFLICT (pipeline_uuid, repo) DO UPDATE SET
		  build_number       = EXCLUDED.build_number,
		  run_number         = EXCLUDED.run_number,
		  creator_name       = EXCLUDED.creator_name,
		  creator_uuid       = EXCLUDED.creator_uuid,
		  creator_account_id = EXCLUDED.creator_account_id,
		  target_ref_type    = EXCLUDED.target_ref_type,
		  target_ref_name    = EXCLUDED.target_ref_name,
		  trigger_name       = EXCLUDED.trigger_name,
		  state_name         = EXCLUDED.state_name,
		  result_name        = EXCLUDED.result_name,
		  created_on         = EXCLUDED.created_on,
		  completed_on       = EXCLUDED.completed_on,
		  build_seconds_used = EXCLUDED.build_seconds_used,
		  raw_json           = EXCLUDED.raw_json
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range pipelines {
		raw, _ := json.Marshal(p)
		completedOn := ""
		if p.CompletedOn != nil {
			completedOn = p.CompletedOn.Format(time.RFC3339)
		}
		if _, err := stmt.ExecContext(ctx,
			p.UUID, repo, p.BuildNumber, p.RunNumber,
			p.Creator.DisplayName, p.Creator.UUID, p.Creator.AccountID,
			p.Target.RefType, p.Target.RefName,
			p.TriggerName(),
			p.State.Name, p.State.Result.Name,
			p.CreatedOn.Format(time.RFC3339), completedOn,
			p.BuildSecondsUsed,
			string(raw),
		); err != nil {
			return fmt.Errorf("upsert pipeline %s: %w", p.UUID, err)
		}
	}
	return tx.Commit()
}

// UpsertDeployments upserts deployment rows for the given repo. Existing rows
// are updated so re-scraping reflects the latest deployment state.
func UpsertDeployments(ctx context.Context, db *sql.DB, repo string, deployments []DeploymentData) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO deployments
		(deployment_uuid, repo, pipeline_uuid, environment_uuid, environment_name,
		 state_name, status_name, release_name, created_on, completed_on, raw_json)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (deployment_uuid, repo) DO UPDATE SET
		  pipeline_uuid    = EXCLUDED.pipeline_uuid,
		  environment_uuid = EXCLUDED.environment_uuid,
		  environment_name = EXCLUDED.environment_name,
		  state_name       = EXCLUDED.state_name,
		  status_name      = EXCLUDED.status_name,
		  release_name     = EXCLUDED.release_name,
		  created_on       = EXCLUDED.created_on,
		  completed_on     = EXCLUDED.completed_on,
		  raw_json         = EXCLUDED.raw_json
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, d := range deployments {
		raw, _ := json.Marshal(d)
		completedOn := ""
		if d.State.CompletedOn != nil {
			completedOn = d.State.CompletedOn.Format(time.RFC3339)
		}
		if _, err := stmt.ExecContext(ctx,
			d.UUID, repo, d.Pipeline.UUID,
			d.Environment.UUID, d.Environment.Name,
			d.State.Name, d.State.Status.Name,
			d.Release.Name,
			d.CreatedOn.Format(time.RFC3339), completedOn,
			string(raw),
		); err != nil {
			return fmt.Errorf("upsert deployment %s: %w", d.UUID, err)
		}
	}
	return tx.Commit()
}

// DeploymentExistsByPipelineUUID returns true when the deployments table already
// contains at least one row for the given pipeline UUID and repo. Used to skip
// the Bitbucket /deployments/{uuid} API call when data is already cached.
func DeploymentExistsByPipelineUUID(ctx context.Context, db *sql.DB, repo, pipelineUUID string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM deployments WHERE repo = $1 AND pipeline_uuid = $2)`,
		repo, pipelineUUID,
	).Scan(&exists)
	return exists, err
}

// UpsertPRPipelines upserts rows into pr_pipelines linking PRs to pipeline UUIDs.
// Existing rows are left unchanged (DO NOTHING) — the link is immutable.
func UpsertPRPipelines(ctx context.Context, db *sql.DB, repo string, links []PRPipelineLink) error {
	if len(links) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pr_pipelines (pr_id, repo, pipeline_uuid)
		VALUES ($1, $2, $3)
		ON CONFLICT (pr_id, repo, pipeline_uuid) DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, l := range links {
		if _, err := stmt.ExecContext(ctx, l.PRID, repo, l.PipelineUUID); err != nil {
			return fmt.Errorf("upsert pr_pipeline pr %d -> %s: %w", l.PRID, l.PipelineUUID, err)
		}
	}
	return tx.Commit()
}

// PipelineExistsByUUID returns true when the pipelines table already contains
// a row for the given pipeline UUID and repo. Used to skip redundant API calls.
func PipelineExistsByUUID(ctx context.Context, db *sql.DB, repo, pipelineUUID string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM pipelines WHERE repo = $1 AND pipeline_uuid = $2)`,
		repo, pipelineUUID,
	).Scan(&exists)
	return exists, err
}

// QueryPRPipelineLinks returns all pr_pipelines rows for a repo, keyed by pr_id.
func QueryPRPipelineLinks(ctx context.Context, db *sql.DB, repo string) (map[int][]string, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT pr_id, pipeline_uuid FROM pr_pipelines WHERE repo = $1", repo)
	if err != nil {
		return nil, fmt.Errorf("query pr_pipelines: %w", err)
	}
	defer rows.Close()

	out := make(map[int][]string)
	for rows.Next() {
		var prID int
		var uuid string
		if err := rows.Scan(&prID, &uuid); err != nil {
			return nil, err
		}
		out[prID] = append(out[prID], uuid)
	}
	return out, rows.Err()
}

// UpsertPullRequests inserts or updates PR rows for the given repo.
func UpsertPullRequests(ctx context.Context, db *sql.DB, repo string, prs []PullRequestData) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pull_requests
		(pr_id, repo, type, title, description, state, comment_count, task_count,
		 author_display_name, author_uuid, author_account_id, author_nickname,
		 src_repo, src_branch, dest_repo, dest_branch, created_on, updated_on,
		 reviewers, participants, raw_json)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
		ON CONFLICT (pr_id, repo) DO UPDATE SET
		  type                = EXCLUDED.type,
		  title               = EXCLUDED.title,
		  description         = EXCLUDED.description,
		  state               = EXCLUDED.state,
		  comment_count       = EXCLUDED.comment_count,
		  task_count          = EXCLUDED.task_count,
		  author_display_name = EXCLUDED.author_display_name,
		  author_uuid         = EXCLUDED.author_uuid,
		  author_account_id   = EXCLUDED.author_account_id,
		  author_nickname     = EXCLUDED.author_nickname,
		  src_repo            = EXCLUDED.src_repo,
		  src_branch          = EXCLUDED.src_branch,
		  dest_repo           = EXCLUDED.dest_repo,
		  dest_branch         = EXCLUDED.dest_branch,
		  created_on          = EXCLUDED.created_on,
		  updated_on          = EXCLUDED.updated_on,
		  reviewers           = EXCLUDED.reviewers,
		  participants        = EXCLUDED.participants,
		  raw_json            = EXCLUDED.raw_json
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, pr := range prs {
		raw, _ := json.Marshal(pr)
		reviewers, _ := json.Marshal(pr.Reviewers)
		participants, _ := json.Marshal(pr.Participants)
		if _, err := stmt.ExecContext(ctx,
			pr.ID, repo, pr.Type, pr.Title, pr.Description, pr.State,
			pr.CommentCount, pr.TaskCount,
			pr.Author.DisplayName, pr.Author.UUID, pr.Author.AccountID, pr.Author.Nickname,
			pr.Source.Repository.FullName, pr.Source.Branch.Name,
			pr.Destination.Repository.FullName, pr.Destination.Branch.Name,
			pr.CreatedOn.Format("2006-01-02"), pr.UpdatedOn.Format("2006-01-02"),
			string(reviewers), string(participants),
			string(raw),
		); err != nil {
			return fmt.Errorf("upsert pull_request %d: %w", pr.ID, err)
		}
	}
	return tx.Commit()
}

// UpsertPRCommits replaces all commit rows for the given repo.
func UpsertPRCommits(ctx context.Context, db *sql.DB, repo string, commits []CommitData) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM pr_commits WHERE repo = $1", repo); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pr_commits
		(pr_id, repo, hash, message, date, author_display_name, author_uuid, author_account_id, raw_json)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range commits {
		raw, _ := json.Marshal(c)
		date := ""
		if !c.Date.IsZero() {
			date = c.Date.Format(time.RFC3339)
		}
		if _, err := stmt.ExecContext(ctx,
			c.PullRequestID, repo, c.Hash, c.Message, date,
			c.Author.User.DisplayName, c.Author.User.UUID, c.Author.User.AccountID,
			string(raw),
		); err != nil {
			return fmt.Errorf("insert pr_commit %s: %w", c.Hash, err)
		}
	}
	return tx.Commit()
}

// UpsertPRActivity replaces all activity rows for the given repo.
func UpsertPRActivity(ctx context.Context, db *sql.DB, repo string, activities []PullRequestActivityData) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM pr_activity WHERE repo = $1", repo); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pr_activity
		(pr_id, repo, activity_type, user_display_name, user_uuid, user_account_id,
		 comment_id, comment_content_raw, comment_created_on, comment_updated_on,
		 approval_date, raw_json)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, act := range activities {
		raw, _ := json.Marshal(act)
		actType, userDisplay, userUUID, userAccountID := "", "", "", ""
		commentID := 0
		commentContent, commentCreatedOn, commentUpdatedOn, approvalDate := "", "", "", ""

		switch {
		case act.Comment.User.DisplayName != "":
			actType = act.Comment.Type
			userDisplay = act.Comment.User.DisplayName
			userUUID = act.Comment.User.UUID
			userAccountID = act.Comment.User.AccountID
			commentID = act.Comment.ID
			commentContent = act.Comment.Content.Raw
			commentCreatedOn = act.Comment.CreatedOn.Format(time.RFC3339)
			commentUpdatedOn = act.Comment.UpdatedOn.Format(time.RFC3339)
		case act.Approval.User.DisplayName != "":
			actType = "approval"
			userDisplay = act.Approval.User.DisplayName
			userUUID = act.Approval.User.UUID
			userAccountID = act.Approval.User.AccountID
			approvalDate = act.Approval.Date.Format(time.RFC3339)
		default:
			actType = "update"
			userDisplay = act.Update.Author.DisplayName
			userUUID = act.Update.Author.UUID
			userAccountID = act.Update.Author.AccountID
		}

		if _, err := stmt.ExecContext(ctx,
			act.PullRequest.ID, repo, actType,
			userDisplay, userUUID, userAccountID,
			commentID, commentContent, commentCreatedOn, commentUpdatedOn,
			approvalDate, string(raw),
		); err != nil {
			return fmt.Errorf("insert pr_activity for pr %d: %w", act.PullRequest.ID, err)
		}
	}
	return tx.Commit()
}

// UpsertPRDiffStat replaces all diffstat rows for the given repo.
func UpsertPRDiffStat(ctx context.Context, db *sql.DB, repo string, diffstats []DiffStatActivityData) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM pr_diffstat WHERE repo = $1", repo); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pr_diffstat
		(pr_id, repo, file_type, lines_added, lines_removed, status, old_path, new_path, raw_json)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, ds := range diffstats {
		raw, _ := json.Marshal(ds)
		if _, err := stmt.ExecContext(ctx,
			ds.PullRequestID, repo, ds.Type,
			ds.LinesAdded, ds.LinesRemoved, ds.Status,
			ds.Old.Path, ds.New.Path,
			string(raw),
		); err != nil {
			return fmt.Errorf("insert pr_diffstat for pr %d: %w", ds.PullRequestID, err)
		}
	}
	return tx.Commit()
}

// UpsertPRComments replaces all comment rows for the given repo.
func UpsertPRComments(ctx context.Context, db *sql.DB, repo string, comments []CommentData) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM pr_comments WHERE repo = $1", repo); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pr_comments
		(pr_id, repo, comment_id, content_raw, inline_path, inline_from, inline_to,
		 parent_id, user_display_name, user_uuid, deleted, created_on, updated_on, raw_json)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range comments {
		raw, _ := json.Marshal(c)
		if _, err := stmt.ExecContext(ctx,
			c.PullRequestID, repo, c.ID,
			c.Content.Raw, c.Inline.Path, c.Inline.From, c.Inline.To,
			c.Parent.ID, c.User.DisplayName, c.User.UUID,
			c.Deleted,
			c.CreatedOn.Format(time.RFC3339), c.UpdatedOn.Format(time.RFC3339),
			string(raw),
		); err != nil {
			return fmt.Errorf("insert pr_comment %d: %w", c.ID, err)
		}
	}
	return tx.Commit()
}

// UpsertPRStatuses replaces all build-status rows for the given repo.
func UpsertPRStatuses(ctx context.Context, db *sql.DB, repo string, statuses []BuildStatus) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM pr_statuses WHERE repo = $1", repo); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pr_statuses
		(pr_id, repo, key, name, state, url, description, created_on, updated_on, raw_json)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, s := range statuses {
		raw, _ := json.Marshal(s)
		if _, err := stmt.ExecContext(ctx,
			s.PullRequestID, repo, s.Key, s.Name, s.State, s.URL, s.Description,
			s.CreatedOn.Format(time.RFC3339), s.UpdatedOn.Format(time.RFC3339),
			string(raw),
		); err != nil {
			return fmt.Errorf("insert pr_status %s: %w", s.Key, err)
		}
	}
	return tx.Commit()
}

// PopulateReportTable rebuilds pr_report rows for the given repo from the
// already-computed reportData map (output of mapPrWithOtherData).
// depByPipelineUUID maps pipeline_uuid → DeploymentData for resolving
// environment/deployment columns; prLinks maps pr_id → []pipeline_uuid.
func PopulateReportTable(
	ctx context.Context,
	db *sql.DB,
	repo string,
	reportData map[int]*PullRequestReportData,
	depByPipelineUUID map[string]DeploymentData,
	prLinks map[int][]string,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM pr_report WHERE repo = $1", repo); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pr_report
		(pr_id, repo, src_repo, src_branch, dest_repo, dest_branch,
		 title, description, state, author, created, updated,
		 file_changed, added, removed, total,
		 activity_type, activity_user, activity_content,
		 pipeline_uuid, environment_name, deployment_state, deployment_status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, rep := range reportData {
		fileChanged, added, removed := 0, 0, 0
		for _, ds := range rep.diffstat {
			fileChanged++
			added += ds.LinesAdded
			removed += ds.LinesRemoved
		}
		total := added + removed

		// Resolve pipeline + deployment data for this PR.
		pipelineUUID, envName, depState, depStatus := "", "", "", ""
		if uuids, ok := prLinks[rep.pr.ID]; ok && len(uuids) > 0 {
			pipelineUUID = uuids[0]
			if dep, ok := depByPipelineUUID[pipelineUUID]; ok {
				envName   = dep.Environment.Name
				depState  = dep.State.Name
				depStatus = dep.State.Status.Name
			}
		}

		pr := rep.pr
		for _, act := range rep.activity {
			var actType, user, content string
			switch {
			case act.Comment.User.DisplayName != "":
				actType = act.Comment.Type
				user = act.Comment.User.DisplayName
				content = act.Comment.Content.Raw
			case act.Approval.User.DisplayName != "":
				actType = "approval"
				user = act.Approval.User.DisplayName
				content = act.Approval.Date.Format("2006-01-02")
			default:
				continue // exclude "update" events from the report
			}

			if _, err := stmt.ExecContext(ctx,
				pr.ID, repo,
				pr.Source.Repository.FullName, pr.Source.Branch.Name,
				pr.Destination.Repository.FullName, pr.Destination.Branch.Name,
				pr.Title, pr.Description, pr.State, pr.Author.DisplayName,
				pr.CreatedOn.Format("2006-01-02"), pr.UpdatedOn.Format("2006-01-02"),
				fileChanged, added, removed, total,
				actType, user, content,
				pipelineUUID, envName, depState, depStatus,
			); err != nil {
				return fmt.Errorf("insert pr_report for pr %d: %w", pr.ID, err)
			}
		}
	}
	return tx.Commit()
}
