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
			activity_type    TEXT,
			activity_user    TEXT,
			activity_content TEXT
		)`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}
	return nil
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
func PopulateReportTable(ctx context.Context, db *sql.DB, repo string, reportData map[int]*PullRequestReportData) error {
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
		 activity_type, activity_user, activity_content)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
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
			); err != nil {
				return fmt.Errorf("insert pr_report for pr %d: %w", pr.ID, err)
			}
		}
	}
	return tx.Commit()
}
