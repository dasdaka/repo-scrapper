package util

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newTestClient builds a Client whose HTTP calls go to srv.
// The URL template uses three %s verbs: workspace, repo, query string.
func newTestClient(srv *httptest.Server, cfg BitbucketConfig) *Client {
	cfg.PullRequestURL = srv.URL + "/%s/%s?q=%s"
	return NewClientWithOptions(cfg, WithHTTPDoer(srv.Client()))
}

// --- buildQueryFilter ---

func TestBuildQueryFilter(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC)

	t.Run("no dates no extra returns empty string", func(t *testing.T) {
		if got := buildQueryFilter(time.Time{}, time.Time{}, ""); got != "" {
			t.Errorf("want empty, got %q", got)
		}
	})

	t.Run("fromDate only produces created_on >= clause", func(t *testing.T) {
		got := buildQueryFilter(from, time.Time{}, "")
		want := `created_on >= "2026-01-01"`
		if got != want {
			t.Errorf("want %q, got %q", want, got)
		}
	})

	t.Run("toDate only produces created_on <= clause", func(t *testing.T) {
		got := buildQueryFilter(time.Time{}, to, "")
		want := `created_on <= "2026-03-31"`
		if got != want {
			t.Errorf("want %q, got %q", want, got)
		}
	})

	t.Run("both dates joined with AND", func(t *testing.T) {
		got := buildQueryFilter(from, to, "")
		want := `created_on >= "2026-01-01" AND created_on <= "2026-03-31"`
		if got != want {
			t.Errorf("want %q, got %q", want, got)
		}
	})

	t.Run("extra filter appended after date clauses", func(t *testing.T) {
		got := buildQueryFilter(from, to, `state="MERGED"`)
		if !strings.Contains(got, " AND ") || !strings.HasSuffix(got, `state="MERGED"`) {
			t.Errorf("unexpected combined filter: %q", got)
		}
	})

	t.Run("extra filter only when no dates", func(t *testing.T) {
		got := buildQueryFilter(time.Time{}, time.Time{}, `state="OPEN"`)
		if got != `state="OPEN"` {
			t.Errorf("want only extra filter, got %q", got)
		}
	})
}

// --- mapPrWithOtherData ---

func TestMapPrWithOtherData(t *testing.T) {
	t.Run("empty inputs produce empty map", func(t *testing.T) {
		res := mapPrWithOtherData(nil, nil, nil)
		if len(res) != 0 {
			t.Errorf("want 0 entries, got %d", len(res))
		}
	})

	t.Run("PRs without activity or diffstat produce empty slices", func(t *testing.T) {
		prs := []PullRequestData{{ID: 1}, {ID: 2}}
		res := mapPrWithOtherData(prs, nil, nil)
		if len(res) != 2 {
			t.Fatalf("want 2 entries, got %d", len(res))
		}
		if len(res[1].activity) != 0 || len(res[1].diffstat) != 0 {
			t.Error("expected empty activity/diffstat slices")
		}
	})

	t.Run("activity is assigned to the correct PR", func(t *testing.T) {
		prs := []PullRequestData{{ID: 10}, {ID: 20}}
		activities := []PullRequestActivityData{{}, {}}
		activities[0].PullRequest.ID = 10
		activities[1].PullRequest.ID = 10

		res := mapPrWithOtherData(prs, activities, nil)
		if got := len(res[10].activity); got != 2 {
			t.Errorf("PR 10: want 2 activities, got %d", got)
		}
		if got := len(res[20].activity); got != 0 {
			t.Errorf("PR 20: want 0 activities, got %d", got)
		}
	})

	t.Run("diffstat is assigned to the correct PR", func(t *testing.T) {
		prs := []PullRequestData{{ID: 5}}
		diffstats := []DiffStatActivityData{
			{PullRequestID: 5, LinesAdded: 10},
			{PullRequestID: 5, LinesAdded: 20},
		}
		res := mapPrWithOtherData(prs, nil, diffstats)
		if got := len(res[5].diffstat); got != 2 {
			t.Errorf("PR 5: want 2 diffstats, got %d", got)
		}
	})

	t.Run("unknown PR ID in activity is silently dropped", func(t *testing.T) {
		prs := []PullRequestData{{ID: 1}}
		act := PullRequestActivityData{}
		act.PullRequest.ID = 999
		res := mapPrWithOtherData(prs, []PullRequestActivityData{act}, nil)
		if len(res[1].activity) != 0 {
			t.Error("expected no activities for PR 1")
		}
	})
}

// --- flatten helpers ---

func TestFlattenPRPages(t *testing.T) {
	pages := []PullRequestList{
		{Values: []PullRequestData{{ID: 1}, {ID: 2}}},
		{Values: []PullRequestData{{ID: 3}}},
	}
	if got := flattenPRPages(pages); len(got) != 3 {
		t.Errorf("want 3, got %d", len(got))
	}
}

func TestFlattenActivityPages(t *testing.T) {
	a1, a2 := PullRequestActivityData{}, PullRequestActivityData{}
	a1.PullRequest.ID = 1
	a2.PullRequest.ID = 2
	pages := []PullRequestActivity{
		{Values: []PullRequestActivityData{a1}},
		{Values: []PullRequestActivityData{a2}},
	}
	if got := flattenActivityPages(pages); len(got) != 2 {
		t.Errorf("want 2, got %d", len(got))
	}
}

func TestFlattenDiffStatPages(t *testing.T) {
	pages := []DiffStatActivity{
		{PullRequestID: 42, Values: []DiffStatActivityData{{LinesAdded: 5}, {LinesAdded: 10}}},
	}
	got := flattenDiffStatPages(pages)
	if len(got) != 2 {
		t.Fatalf("want 2, got %d", len(got))
	}
	for _, d := range got {
		if d.PullRequestID != 42 {
			t.Errorf("PullRequestID: want 42, got %d", d.PullRequestID)
		}
	}
}

// --- min ---

func TestMin(t *testing.T) {
	cases := []struct{ a, b, want int }{
		{1, 2, 1}, {2, 1, 1}, {5, 5, 5}, {0, -1, -1},
	}
	for _, tc := range cases {
		if got := min(tc.a, tc.b); got != tc.want {
			t.Errorf("min(%d,%d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// --- parseRetryAfter ---

func TestParseRetryAfter(t *testing.T) {
	t.Run("empty string returns 0", func(t *testing.T) {
		if d := parseRetryAfter(""); d != 0 {
			t.Errorf("want 0, got %s", d)
		}
	})
	t.Run("integer seconds", func(t *testing.T) {
		if d := parseRetryAfter("5"); d != 5*time.Second {
			t.Errorf("want 5s, got %s", d)
		}
	})
}

// --- fetchAllPages ---

func TestFetchAllPages_FollowsNextLinks(t *testing.T) {
	page := 0
	responses := []PullRequestList{
		{Values: []PullRequestData{{ID: 1}, {ID: 2}}},
		{Values: []PullRequestData{{ID: 3}}},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := responses[page]
		page++
		if page < len(responses) {
			resp.Next = "http://" + r.Host + r.URL.Path + "?page=next"
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := NewClientWithOptions(BitbucketConfig{Token: "Bearer t"}, WithHTTPDoer(srv.Client()))
	prs, err := fetchAllPages[PullRequestData](context.Background(), c, srv.URL+"/prs", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(prs) != 3 {
		t.Errorf("want 3 PRs across 2 pages, got %d", len(prs))
	}
}

// --- retry on 429 ---

func TestRetry_On429(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(PullRequestList{Values: []PullRequestData{{ID: 42}}})
	}))
	defer srv.Close()

	cfg := BitbucketConfig{
		Token:          "Bearer t",
		Workspace:      "ws",
		PullRequestURL: srv.URL + "/%s/%s?q=%s",
	}
	c := NewClientWithOptions(cfg,
		WithHTTPDoer(srv.Client()),
		WithRetry(RetryConfig{MaxAttempts: 4, BaseBackoff: 0, MaxBackoff: 0}),
	)

	prs, err := c.fetchAllPullRequestList(context.Background(), "my-repo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(prs) != 1 || prs[0].ID != 42 {
		t.Errorf("unexpected PRs: %v", prs)
	}
	if attempts != 3 {
		t.Errorf("want 3 attempts (2 retries), got %d", attempts)
	}
}

func TestRetry_ExhaustedReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	cfg := BitbucketConfig{
		Token:          "Bearer t",
		Workspace:      "ws",
		PullRequestURL: srv.URL + "/%s/%s?q=%s",
	}
	c := NewClientWithOptions(cfg,
		WithHTTPDoer(srv.Client()),
		WithRetry(RetryConfig{MaxAttempts: 2, BaseBackoff: 0, MaxBackoff: 0}),
	)

	if _, err := c.fetchAllPullRequestList(context.Background(), "repo"); err == nil {
		t.Error("expected error after exhausted retries")
	}
}

// --- HTTP fetch tests ---

func TestFetchPullRequestList_SinglePage(t *testing.T) {
	want := []PullRequestData{{ID: 1, Title: "Fix bug"}, {ID: 2, Title: "Feature"}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(PullRequestList{Values: want})
	}))
	defer srv.Close()

	cfg := BitbucketConfig{
		Token:          "Bearer test",
		Workspace:      "ws",
		PullRequestURL: srv.URL + "/%s/%s?q=%s",
	}
	c := NewClientWithOptions(cfg, WithHTTPDoer(srv.Client()))

	prs, err := c.fetchAllPullRequestList(context.Background(), "my-repo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(prs) != 2 || prs[0].ID != 1 || prs[1].ID != 2 {
		t.Errorf("unexpected PRs: %v", prs)
	}
}

func TestFetchPullRequestList_ErrorResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error": "unauthorized"}`))
	}))
	defer srv.Close()

	cfg := BitbucketConfig{
		Token:          "Bearer bad",
		Workspace:      "ws",
		PullRequestURL: srv.URL + "/%s/%s?q=%s",
	}
	c := NewClientWithOptions(cfg, WithHTTPDoer(srv.Client()))
	// Non-2xx responses other than 429 are not treated as transport errors.
	if _, err := c.fetchAllPullRequestList(context.Background(), "repo"); err != nil {
		t.Errorf("unexpected transport error: %v", err)
	}
}

func TestFetchAllActivity(t *testing.T) {
	want := PullRequestActivity{
		Values: []PullRequestActivityData{
			func() PullRequestActivityData {
				var a PullRequestActivityData
				a.Approval.User.DisplayName = "Alice"
				a.Approval.Date = time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
				return a
			}(),
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	c := NewClientWithOptions(BitbucketConfig{Token: "Bearer t"}, WithHTTPDoer(srv.Client()))
	pr := PullRequestData{ID: 7}
	pr.Links.Activity.Href = srv.URL + "/activity"

	got, err := c.fetchAllActivity(context.Background(), pr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 activity, got %d", len(got))
	}
	if got[0].Approval.User.DisplayName != "Alice" {
		t.Errorf("want Alice, got %q", got[0].Approval.User.DisplayName)
	}
	if got[0].PullRequest.ID != 7 {
		t.Errorf("PR ID not stamped: want 7, got %d", got[0].PullRequest.ID)
	}
}

func TestParseDiff(t *testing.T) {
	cases := []struct {
		name    string
		diff    string
		added   int
		removed int
	}{
		{
			name:    "basic hunk",
			diff:    "--- a/x.go\n+++ b/x.go\n@@ -1 +1,2 @@\n-old\n+new\n+extra\n",
			added:   2,
			removed: 1,
		},
		{
			name:    "empty diff",
			diff:    "",
			added:   0,
			removed: 0,
		},
		{
			name:    "binary file marker counts nothing",
			diff:    "Binary files a/img.png and b/img.png differ\n",
			added:   0,
			removed: 0,
		},
		{
			name:    "multiple files",
			diff:    "--- a/a.go\n+++ b/a.go\n@@ -1 +1 @@\n-old\n+new\n--- a/b.go\n+++ b/b.go\n@@ -1 +1,2 @@\n unchanged\n+added\n",
			added:   2,
			removed: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got_a, got_r := parseDiff([]byte(tc.diff))
			if got_a != tc.added || got_r != tc.removed {
				t.Errorf("want added=%d removed=%d, got added=%d removed=%d",
					tc.added, tc.removed, got_a, got_r)
			}
		})
	}
}

func TestFetchDiffStat(t *testing.T) {
	rawDiff := "--- a/x.go\n+++ b/x.go\n@@ -1 +1,2 @@\n-old\n+new\n+extra\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/x-diff")
		w.Write([]byte(rawDiff))
	}))
	defer srv.Close()

	c := NewClientWithOptions(BitbucketConfig{Token: "Bearer t"}, WithHTTPDoer(srv.Client()))
	pr := PullRequestData{ID: 99}
	pr.Links.Diff.Href = srv.URL + "/diff"

	got, err := c.fetchDiffStat(context.Background(), pr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 result, got %d", len(got))
	}
	if got[0].PullRequestID != 99 {
		t.Errorf("PullRequestID: want 99, got %d", got[0].PullRequestID)
	}
	if got[0].LinesAdded != 2 {
		t.Errorf("LinesAdded: want 2, got %d", got[0].LinesAdded)
	}
	if got[0].LinesRemoved != 1 {
		t.Errorf("LinesRemoved: want 1, got %d", got[0].LinesRemoved)
	}
}

func TestFetchAllPRData_Concurrent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{"values": []interface{}{}})
	}))
	defer srv.Close()

	c := NewClientWithOptions(
		BitbucketConfig{Token: "Bearer t"},
		WithHTTPDoer(srv.Client()),
		WithConcurrency(3),
	)

	makePR := func(id int) PullRequestData {
		pr := PullRequestData{ID: id}
		pr.Links.Commits.Href = srv.URL + "/commits"
		pr.Links.Activity.Href = srv.URL + "/activity"
		pr.Links.Diff.Href = srv.URL + "/diff"
		pr.Links.Comments.Href = srv.URL + "/comments"
		pr.Links.Statuses.Href = srv.URL + "/statuses"
		return pr
	}

	prs := make([]PullRequestData, 5)
	for i := range prs {
		prs[i] = makePR(i + 1)
	}

	results := make([]prSubData, len(prs))
	for i, pr := range prs {
		c.sem.acquire()
		data, err := c.fetchAllPRData(context.Background(), pr)
		c.sem.release()
		if err != nil {
			t.Fatalf("PR %d: %v", i+1, err)
		}
		results[i] = data
	}

	for i, r := range results {
		if r.pr.ID != i+1 {
			t.Errorf("result[%d].pr.ID = %d", i, r.pr.ID)
		}
	}
}
