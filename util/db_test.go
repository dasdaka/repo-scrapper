package util

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

const defaultTestDSN = "postgres://postgres:postgres@localhost:5432/repo_scrapper_test?sslmode=disable"

// openTestDB connects to the test PostgreSQL database, creates the schema,
// and truncates all tables so each test starts clean.
// Set TEST_DB_DSN to override the default connection string.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = defaultTestDSN
	}
	db, err := OpenDB(dsn)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := CreateSchema(context.Background(), db); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}
	// Truncate all tables before each test for isolation.
	_, err = db.Exec(`TRUNCATE pull_requests, pr_commits, pr_activity,
		pr_diffstat, pr_comments, pr_statuses, pr_report RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	return db
}

func TestCreateSchema(t *testing.T) {
	db := openTestDB(t)

	tables := []string{
		"pull_requests", "pr_commits", "pr_activity",
		"pr_diffstat", "pr_comments", "pr_statuses", "pr_report",
	}
	for _, tbl := range tables {
		var name string
		err := db.QueryRow(
			"SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name=$1", tbl,
		).Scan(&name)
		if err != nil || name != tbl {
			t.Errorf("table %q not found: %v", tbl, err)
		}
	}
}

func TestCreateSchema_Idempotent(t *testing.T) {
	db := openTestDB(t)
	// Running it twice must not error.
	if err := CreateSchema(context.Background(), db); err != nil {
		t.Errorf("second CreateSchema call failed: %v", err)
	}
}

// --- UpsertPullRequests ---

func makePR(id int, title, state, repo string) PullRequestData {
	pr := PullRequestData{
		ID:    id,
		Title: title,
		State: state,
	}
	pr.Author.DisplayName = "Author"
	pr.Source.Repository.FullName = repo + "/src"
	pr.Source.Branch.Name = "feature"
	pr.Destination.Repository.FullName = repo + "/dst"
	pr.Destination.Branch.Name = "main"
	pr.CreatedOn = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pr.UpdatedOn = time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	return pr
}

func TestUpsertPullRequests(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	prs := []PullRequestData{
		makePR(1, "First PR", "MERGED", "ws/repo"),
		makePR(2, "Second PR", "OPEN", "ws/repo"),
	}

	if err := UpsertPullRequests(ctx, db, "repo", prs); err != nil {
		t.Fatalf("UpsertPullRequests: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pull_requests WHERE repo='repo'").Scan(&count)
	if count != 2 {
		t.Errorf("want 2 rows, got %d", count)
	}
}

func TestUpsertPullRequests_Replace(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	pr := makePR(1, "Original", "OPEN", "ws/repo")
	UpsertPullRequests(ctx, db, "repo", []PullRequestData{pr})

	pr.Title = "Updated"
	pr.State = "MERGED"
	if err := UpsertPullRequests(ctx, db, "repo", []PullRequestData{pr}); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	var title, state string
	db.QueryRow("SELECT title, state FROM pull_requests WHERE pr_id=1").Scan(&title, &state)
	if title != "Updated" || state != "MERGED" {
		t.Errorf("want Updated/MERGED, got %q/%q", title, state)
	}
}

func TestUpsertPullRequests_ReviewersStored(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	pr := makePR(1, "PR", "OPEN", "ws/repo")
	pr.Reviewers = []PRParticipant{{DisplayName: "Alice", UUID: "uuid-alice"}}
	UpsertPullRequests(ctx, db, "repo", []PullRequestData{pr})

	var reviewers string
	db.QueryRow("SELECT reviewers FROM pull_requests WHERE pr_id=1").Scan(&reviewers)
	if reviewers == "" || reviewers == "null" {
		t.Errorf("reviewers column should contain JSON, got %q", reviewers)
	}
}

// --- UpsertPRActivity ---

func makeApprovalActivity(prID int, user string) PullRequestActivityData {
	var a PullRequestActivityData
	a.PullRequest.ID = prID
	a.Approval.User.DisplayName = user
	a.Approval.User.UUID = "uuid-" + user
	a.Approval.Date = time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	return a
}

func makeCommentActivity(prID int, user, content string) PullRequestActivityData {
	var a PullRequestActivityData
	a.PullRequest.ID = prID
	a.Comment.User.DisplayName = user
	a.Comment.Type = "pullrequest_comment"
	a.Comment.Content.Raw = content
	a.Comment.CreatedOn = time.Now()
	a.Comment.UpdatedOn = time.Now()
	return a
}

func TestUpsertPRActivity(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	activities := []PullRequestActivityData{
		makeApprovalActivity(1, "Alice"),
		makeCommentActivity(1, "Bob", "LGTM"),
	}

	if err := UpsertPRActivity(ctx, db, "repo", activities); err != nil {
		t.Fatalf("UpsertPRActivity: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_activity WHERE repo='repo'").Scan(&count)
	if count != 2 {
		t.Errorf("want 2 rows, got %d", count)
	}
}

func TestUpsertPRActivity_DeletesExisting(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	UpsertPRActivity(ctx, db, "repo", []PullRequestActivityData{
		makeApprovalActivity(1, "Alice"),
		makeApprovalActivity(2, "Bob"),
	})

	// Re-upsert with only one entry — old rows must be gone.
	if err := UpsertPRActivity(ctx, db, "repo", []PullRequestActivityData{
		makeApprovalActivity(3, "Charlie"),
	}); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_activity WHERE repo='repo'").Scan(&count)
	if count != 1 {
		t.Errorf("want 1 row after replace, got %d", count)
	}
}

func TestUpsertPRActivity_ActivityTypes(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	var updateAct PullRequestActivityData
	updateAct.PullRequest.ID = 1
	updateAct.Update.Author.DisplayName = "Dave"

	activities := []PullRequestActivityData{
		makeApprovalActivity(1, "Alice"),
		makeCommentActivity(1, "Bob", "Looks good"),
		updateAct,
	}
	UpsertPRActivity(ctx, db, "repo", activities)

	rows, _ := db.Query("SELECT activity_type FROM pr_activity WHERE repo='repo'")
	defer rows.Close()
	types := map[string]int{}
	for rows.Next() {
		var t string
		rows.Scan(&t)
		types[t]++
	}
	if types["approval"] != 1 || types["pullrequest_comment"] != 1 || types["update"] != 1 {
		t.Errorf("unexpected activity type counts: %v", types)
	}
}

// --- UpsertPRDiffStat ---

func TestUpsertPRDiffStat(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	diffstats := []DiffStatActivityData{
		{PullRequestID: 1, LinesAdded: 10, LinesRemoved: 2, Status: "modified"},
		{PullRequestID: 1, LinesAdded: 5, LinesRemoved: 0, Status: "added"},
	}

	if err := UpsertPRDiffStat(ctx, db, "repo", diffstats); err != nil {
		t.Fatalf("UpsertPRDiffStat: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_diffstat WHERE repo='repo'").Scan(&count)
	if count != 2 {
		t.Errorf("want 2 rows, got %d", count)
	}
}

func TestUpsertPRDiffStat_DeletesExisting(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	UpsertPRDiffStat(ctx, db, "repo", []DiffStatActivityData{
		{PullRequestID: 1, LinesAdded: 1},
		{PullRequestID: 2, LinesAdded: 2},
	})
	UpsertPRDiffStat(ctx, db, "repo", []DiffStatActivityData{
		{PullRequestID: 3, LinesAdded: 3},
	})

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_diffstat WHERE repo='repo'").Scan(&count)
	if count != 1 {
		t.Errorf("want 1 row, got %d", count)
	}
}

// --- UpsertPRCommits ---

func TestUpsertPRCommits(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	commits := []CommitData{
		{Hash: "abc123", Message: "init", PullRequestID: 1},
		{Hash: "def456", Message: "fix", PullRequestID: 1},
	}

	if err := UpsertPRCommits(ctx, db, "repo", commits); err != nil {
		t.Fatalf("UpsertPRCommits: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_commits WHERE repo='repo'").Scan(&count)
	if count != 2 {
		t.Errorf("want 2 rows, got %d", count)
	}
}

func TestUpsertPRCommits_DeletesExisting(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	UpsertPRCommits(ctx, db, "repo", []CommitData{
		{Hash: "old1", PullRequestID: 1},
		{Hash: "old2", PullRequestID: 1},
	})
	if err := UpsertPRCommits(ctx, db, "repo", []CommitData{
		{Hash: "new1", PullRequestID: 2},
	}); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_commits WHERE repo='repo'").Scan(&count)
	if count != 1 {
		t.Errorf("want 1 row after replace, got %d", count)
	}
}

// --- UpsertPRComments ---

func TestUpsertPRComments(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	comments := []CommentData{
		{ID: 1, PullRequestID: 10, Content: struct {
			Raw    string `json:"raw"`
			Markup string `json:"markup"`
			HTML   string `json:"html"`
		}{Raw: "Looks good"}},
	}

	if err := UpsertPRComments(ctx, db, "repo", comments); err != nil {
		t.Fatalf("UpsertPRComments: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_comments WHERE repo='repo'").Scan(&count)
	if count != 1 {
		t.Errorf("want 1 row, got %d", count)
	}
}

// --- UpsertPRStatuses ---

func TestUpsertPRStatuses(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	statuses := []BuildStatus{
		{Key: "ci/test", Name: "Tests", State: "SUCCESSFUL", PullRequestID: 1},
		{Key: "ci/lint", Name: "Lint", State: "FAILED", PullRequestID: 1},
	}

	if err := UpsertPRStatuses(ctx, db, "repo", statuses); err != nil {
		t.Fatalf("UpsertPRStatuses: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_statuses WHERE repo='repo'").Scan(&count)
	if count != 2 {
		t.Errorf("want 2 rows, got %d", count)
	}
}

// --- PopulateReportTable ---

func buildReportData() map[int]*PullRequestReportData {
	pr := makePR(1, "Test PR", "MERGED", "ws/repo")
	approval := makeApprovalActivity(1, "Alice")
	comment := makeCommentActivity(1, "Bob", "Looks good")

	return map[int]*PullRequestReportData{
		1: {
			pr:       pr,
			activity: []PullRequestActivityData{approval, comment},
			diffstat: []DiffStatActivityData{
				{PullRequestID: 1, LinesAdded: 20, LinesRemoved: 5},
			},
		},
	}
}

func TestPopulateReportTable(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	if err := PopulateReportTable(ctx, db, "repo", buildReportData(), nil, nil); err != nil {
		t.Fatalf("PopulateReportTable: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_report WHERE repo='repo'").Scan(&count)
	// One PR with 2 activities (approval + comment) = 2 rows.
	if count != 2 {
		t.Errorf("want 2 rows, got %d", count)
	}
}

func TestPopulateReportTable_ExcludesUpdates(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	pr := makePR(1, "PR", "MERGED", "ws/repo")
	var updateAct PullRequestActivityData
	updateAct.PullRequest.ID = 1
	updateAct.Update.Author.DisplayName = "Dave"

	data := map[int]*PullRequestReportData{
		1: {pr: pr, activity: []PullRequestActivityData{updateAct}},
	}

	PopulateReportTable(ctx, db, "repo", data, nil, nil)

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_report WHERE repo='repo'").Scan(&count)
	if count != 0 {
		t.Errorf("update events must be excluded; got %d rows", count)
	}
}

func TestPopulateReportTable_AggregatesDiffStats(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	pr := makePR(1, "PR", "MERGED", "ws/repo")
	approval := makeApprovalActivity(1, "Alice")

	data := map[int]*PullRequestReportData{
		1: {
			pr:       pr,
			activity: []PullRequestActivityData{approval},
			diffstat: []DiffStatActivityData{
				{PullRequestID: 1, LinesAdded: 10, LinesRemoved: 3},
				{PullRequestID: 1, LinesAdded: 5, LinesRemoved: 1},
			},
		},
	}

	PopulateReportTable(ctx, db, "repo", data, nil, nil)

	var added, removed, total int
	db.QueryRow("SELECT added, removed, total FROM pr_report WHERE repo='repo'").
		Scan(&added, &removed, &total)
	if added != 15 || removed != 4 || total != 19 {
		t.Errorf("want added=15, removed=4, total=19; got %d/%d/%d", added, removed, total)
	}
}

func TestPopulateReportTable_DeletesExisting(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	PopulateReportTable(ctx, db, "repo", buildReportData(), nil, nil)
	// Re-run: old rows must be deleted first.
	PopulateReportTable(ctx, db, "repo", buildReportData(), nil, nil)

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_report WHERE repo='repo'").Scan(&count)
	if count != 2 {
		t.Errorf("want 2 rows after idempotent repopulate, got %d", count)
	}
}
