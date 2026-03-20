package util

import (
	"context"
	"encoding/json"
	"testing"
)

func TestAggregate_RebuildsPRReport(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	// Build raw data and insert into raw tables using the same helpers as db_test.go.
	pr := makePR(1, "Test PR", "MERGED", "ws/repo")
	if err := UpsertPullRequests(ctx, db, "repo", []PullRequestData{pr}); err != nil {
		t.Fatalf("UpsertPullRequests: %v", err)
	}

	approval := makeApprovalActivity(1, "Alice")
	if err := UpsertPRActivity(ctx, db, "repo", []PullRequestActivityData{approval}); err != nil {
		t.Fatalf("UpsertPRActivity: %v", err)
	}

	if err := UpsertPRDiffStat(ctx, db, "repo", []DiffStatActivityData{
		{PullRequestID: 1, LinesAdded: 10, LinesRemoved: 3},
	}); err != nil {
		t.Fatalf("UpsertPRDiffStat: %v", err)
	}

	// Aggregate should produce one pr_report row.
	if err := Aggregate(ctx, db, []string{"repo"}, NopLog); err != nil {
		t.Fatalf("Aggregate: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_report WHERE repo='repo'").Scan(&count)
	if count != 1 {
		t.Errorf("want 1 row in pr_report, got %d", count)
	}
}

func TestAggregate_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	pr := makePR(1, "PR", "MERGED", "ws/repo")
	UpsertPullRequests(ctx, db, "repo", []PullRequestData{pr})
	UpsertPRActivity(ctx, db, "repo", []PullRequestActivityData{makeApprovalActivity(1, "Bob")})
	UpsertPRDiffStat(ctx, db, "repo", []DiffStatActivityData{{PullRequestID: 1, LinesAdded: 5}})

	Aggregate(ctx, db, []string{"repo"}, NopLog)
	Aggregate(ctx, db, []string{"repo"}, NopLog) // second run must not duplicate rows

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_report WHERE repo='repo'").Scan(&count)
	if count != 1 {
		t.Errorf("want 1 row after two aggregations, got %d", count)
	}
}

func TestAggregate_LoadsCorrectDiffStatTotals(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	pr := makePR(1, "PR", "MERGED", "ws/repo")
	UpsertPullRequests(ctx, db, "repo", []PullRequestData{pr})
	UpsertPRActivity(ctx, db, "repo", []PullRequestActivityData{makeApprovalActivity(1, "Alice")})
	UpsertPRDiffStat(ctx, db, "repo", []DiffStatActivityData{
		{PullRequestID: 1, LinesAdded: 7, LinesRemoved: 2},
		{PullRequestID: 1, LinesAdded: 3, LinesRemoved: 1},
	})

	Aggregate(ctx, db, []string{"repo"}, NopLog)

	var added, removed, total int
	db.QueryRow("SELECT added, removed, total FROM pr_report WHERE repo='repo'").
		Scan(&added, &removed, &total)
	if added != 10 || removed != 3 || total != 13 {
		t.Errorf("want 10/3/13, got %d/%d/%d", added, removed, total)
	}
}

// TestLoadReportData_RoundTrip verifies that queryPullRequests reconstructs the
// PullRequestData correctly from stored raw_json.
func TestLoadReportData_RoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	pr := makePR(42, "Round-trip PR", "OPEN", "ws/repo")
	pr.Reviewers = []PRParticipant{{DisplayName: "Reviewer", UUID: "uuid-r"}}

	if err := UpsertPullRequests(ctx, db, "repo", []PullRequestData{pr}); err != nil {
		t.Fatalf("UpsertPullRequests: %v", err)
	}

	prs, err := queryPullRequests(ctx, db, "repo")
	if err != nil {
		t.Fatalf("queryPullRequests: %v", err)
	}
	if len(prs) != 1 {
		t.Fatalf("want 1 PR, got %d", len(prs))
	}
	if prs[0].ID != 42 || prs[0].Title != "Round-trip PR" {
		t.Errorf("unexpected PR: %+v", prs[0])
	}
	if len(prs[0].Reviewers) != 1 || prs[0].Reviewers[0].DisplayName != "Reviewer" {
		t.Errorf("reviewers not round-tripped: %+v", prs[0].Reviewers)
	}
}

// TestLoadReportData_DiffStatPRID verifies that PullRequestID is restored from
// the pr_id column (it is not part of raw_json).
func TestLoadReportData_DiffStatPRID(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	if err := UpsertPRDiffStat(ctx, db, "repo", []DiffStatActivityData{
		{PullRequestID: 7, LinesAdded: 5, Status: "modified"},
	}); err != nil {
		t.Fatalf("UpsertPRDiffStat: %v", err)
	}

	diffs, err := queryDiffStats(ctx, db, "repo")
	if err != nil {
		t.Fatalf("queryDiffStats: %v", err)
	}
	if len(diffs) != 1 || diffs[0].PullRequestID != 7 {
		t.Errorf("want PullRequestID=7, got %+v", diffs)
	}
}

// TestAggregate_ActivityRoundTrip checks that activity data stored as JSON is
// correctly reconstructed and used during aggregation.
func TestAggregate_ActivityRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	pr := makePR(1, "PR", "MERGED", "ws/repo")
	UpsertPullRequests(ctx, db, "repo", []PullRequestData{pr})

	// Store a comment activity — the PullRequest.ID must survive the JSON round-trip.
	act := makeCommentActivity(1, "Carol", "Looks good")
	raw, _ := json.Marshal(act)

	// Manually verify the JSON contains pull_request.id = 1 before storing.
	var check map[string]interface{}
	json.Unmarshal(raw, &check)
	prField, _ := check["pull_request"].(map[string]interface{})
	if id, _ := prField["id"].(float64); id != 1 {
		t.Fatalf("activity JSON does not contain pull_request.id=1, check makeCommentActivity: %s", raw)
	}

	UpsertPRActivity(ctx, db, "repo", []PullRequestActivityData{act})

	activities, err := queryActivities(ctx, db, "repo")
	if err != nil {
		t.Fatalf("queryActivities: %v", err)
	}
	if len(activities) != 1 {
		t.Fatalf("want 1 activity, got %d", len(activities))
	}
	if activities[0].PullRequest.ID != 1 {
		t.Errorf("PullRequest.ID not round-tripped; want 1, got %d", activities[0].PullRequest.ID)
	}

	// Full aggregate should produce one pr_report row (comment, not update).
	UpsertPRDiffStat(ctx, db, "repo", nil)
	Aggregate(ctx, db, []string{"repo"}, NopLog)

	var count int
	db.QueryRow("SELECT COUNT(*) FROM pr_report WHERE repo='repo'").Scan(&count)
	if count != 1 {
		t.Errorf("want 1 report row, got %d", count)
	}
}

