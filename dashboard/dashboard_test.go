package dashboard

import (
	"testing"
	"time"
)

// --- shortRepo ---

func TestShortRepo(t *testing.T) {
	cases := []struct {
		input, want string
	}{
		{"workspace/my-repo", "my-repo"},
		{"org/some-service", "some-service"},
		{"noslash", "noslash"},
		{"", ""},
		{"a/b/c", "b/c"},
	}
	for _, tc := range cases {
		if got := shortRepo(tc.input); got != tc.want {
			t.Errorf("shortRepo(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// --- toSet ---

func TestToSet(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		if toSet(nil) != nil {
			t.Error("expected nil")
		}
	})
	t.Run("empty slice returns nil", func(t *testing.T) {
		if toSet([]string{}) != nil {
			t.Error("expected nil")
		}
	})
	t.Run("non-empty slice returns map", func(t *testing.T) {
		s := toSet([]string{"a", "b", "a"})
		if !s["a"] || !s["b"] {
			t.Error("expected a and b in set")
		}
		if len(s) != 2 {
			t.Errorf("want 2 unique keys, got %d", len(s))
		}
	})
	t.Run("blank strings are excluded", func(t *testing.T) {
		s := toSet([]string{"", "x"})
		if s[""] {
			t.Error("blank string must not be in set")
		}
	})
}

// --- helpers ---

func date(s string) time.Time {
	t, _ := time.Parse("2006-01-02", s)
	return t
}

func makeRow(id int, srcRepo, author, typ, user string, updated time.Time) ActivityRow {
	return ActivityRow{
		ID:      id,
		SrcRepo: srcRepo,
		Author:  author,
		Type:    typ,
		User:    user,
		Updated: updated,
		Added:   10,
		Removed: 2,
		Total:   12,
	}
}

// --- filterActivities ---

func TestFilterActivities(t *testing.T) {
	rows := []ActivityRow{
		makeRow(1, "ws/repo-a", "Alice", "approval", "Bob", date("2024-01-10")),
		makeRow(2, "ws/repo-a", "Alice", "approval", "Carol", date("2024-02-15")),
		makeRow(3, "ws/repo-b", "Dave", "pullrequest_comment", "Eve", date("2024-03-20")),
	}

	t.Run("no filter returns all rows", func(t *testing.T) {
		got := filterActivities(rows, FilterParams{}, nil)
		if len(got) != 3 {
			t.Errorf("want 3, got %d", len(got))
		}
	})

	t.Run("dateFrom filters out older rows", func(t *testing.T) {
		got := filterActivities(rows, FilterParams{DateFrom: date("2024-02-01")}, nil)
		if len(got) != 2 {
			t.Errorf("want 2, got %d", len(got))
		}
	})

	t.Run("dateTo filters out newer rows", func(t *testing.T) {
		got := filterActivities(rows, FilterParams{DateTo: date("2024-02-01")}, nil)
		if len(got) != 1 {
			t.Errorf("want 1, got %d", len(got))
		}
	})

	t.Run("date range keeps middle row only", func(t *testing.T) {
		got := filterActivities(rows, FilterParams{
			DateFrom: date("2024-02-01"),
			DateTo:   date("2024-02-28"),
		}, nil)
		if len(got) != 1 || got[0].ID != 2 {
			t.Errorf("want row ID=2, got %v", got)
		}
	})

	t.Run("repo filter", func(t *testing.T) {
		got := filterActivities(rows, FilterParams{Repos: []string{"repo-b"}}, nil)
		if len(got) != 1 || got[0].ID != 3 {
			t.Errorf("want row 3, got %v", got)
		}
	})

	t.Run("author filter", func(t *testing.T) {
		got := filterActivities(rows, FilterParams{Authors: []string{"Dave"}}, nil)
		if len(got) != 1 || got[0].ID != 3 {
			t.Errorf("want row 3, got %v", got)
		}
	})

	t.Run("combined repo and author filter", func(t *testing.T) {
		got := filterActivities(rows, FilterParams{
			Repos:   []string{"repo-a"},
			Authors: []string{"Alice"},
		}, nil)
		if len(got) != 2 {
			t.Errorf("want 2, got %d", len(got))
		}
	})

	t.Run("no match returns nil", func(t *testing.T) {
		got := filterActivities(rows, FilterParams{Authors: []string{"NoOne"}}, nil)
		if len(got) != 0 {
			t.Errorf("want 0, got %d", len(got))
		}
	})

	t.Run("excluded author is hidden from all results", func(t *testing.T) {
		bots := map[string]bool{"Alice": true}
		got := filterActivities(rows, FilterParams{}, bots)
		for _, r := range got {
			if r.Author == "Alice" {
				t.Errorf("bot author Alice should be excluded, got row ID=%d", r.ID)
			}
		}
		if len(got) != 1 || got[0].Author != "Dave" {
			t.Errorf("want only Dave's row, got %v", got)
		}
	})

	t.Run("excluded user is hidden from all results", func(t *testing.T) {
		// Bob is a bot reviewer; rows where User=Bob should be dropped.
		bots := map[string]bool{"Bob": true}
		got := filterActivities(rows, FilterParams{}, bots)
		for _, r := range got {
			if r.User == "Bob" {
				t.Errorf("bot user Bob should be excluded, got row ID=%d", r.ID)
			}
		}
		// row 1 has User=Bob, rows 2 and 3 have User=Carol/Eve — only 2 rows remain.
		if len(got) != 2 {
			t.Errorf("want 2 rows after bot-user exclusion, got %d", len(got))
		}
	})
}

// --- deduplicatePRs ---

func TestDeduplicatePRs(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		if got := deduplicatePRs(nil); got != nil {
			t.Error("expected nil")
		}
	})

	t.Run("each ID appears once", func(t *testing.T) {
		rows := []ActivityRow{
			makeRow(1, "ws/repo", "Alice", "approval", "Bob", date("2024-01-01")),
			makeRow(1, "ws/repo", "Alice", "pullrequest_comment", "Carol", date("2024-01-01")),
			makeRow(2, "ws/repo", "Dave", "approval", "Eve", date("2024-01-02")),
		}
		prs := deduplicatePRs(rows)
		if len(prs) != 2 {
			t.Errorf("want 2 unique PRs, got %d", len(prs))
		}
	})

	t.Run("repo name is stripped of workspace prefix", func(t *testing.T) {
		rows := []ActivityRow{makeRow(1, "ws/my-repo", "Alice", "approval", "Bob", date("2024-01-01"))}
		prs := deduplicatePRs(rows)
		if prs[0].SrcRepo != "my-repo" {
			t.Errorf("want my-repo, got %q", prs[0].SrcRepo)
		}
	})

	t.Run("first occurrence wins for code stats", func(t *testing.T) {
		rows := []ActivityRow{
			{ID: 5, SrcRepo: "ws/r", Added: 100, Removed: 50, Total: 150, Updated: date("2024-01-01")},
			{ID: 5, SrcRepo: "ws/r", Added: 999, Removed: 999, Total: 999, Updated: date("2024-01-01")},
		}
		prs := deduplicatePRs(rows)
		if prs[0].Added != 100 {
			t.Errorf("want Added=100 from first row, got %d", prs[0].Added)
		}
	})
}

// --- BuildMeta ---

func TestBuildMeta(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		m := BuildMeta(nil, nil, nil)
		if m.DateMin != "" || m.DateMax != "" {
			t.Error("expected empty date bounds for empty input")
		}
	})

	t.Run("populates repos, authors, users", func(t *testing.T) {
		rows := []ActivityRow{
			{SrcRepo: "ws/repo-a", Author: "Alice", User: "Bob", Updated: date("2024-01-01")},
			{SrcRepo: "ws/repo-b", Author: "Charlie", User: "Alice", Updated: date("2024-06-01")},
		}
		m := BuildMeta(rows, nil, nil)

		if len(m.Repos) != 2 {
			t.Errorf("want 2 repos, got %d", len(m.Repos))
		}
		if len(m.Authors) != 2 {
			t.Errorf("want 2 authors, got %d", len(m.Authors))
		}
		// Alice appears as both author and user
		if len(m.Users) != 2 {
			t.Errorf("want 2 users, got %d", len(m.Users))
		}
	})

	t.Run("date bounds", func(t *testing.T) {
		rows := []ActivityRow{
			makeRow(1, "ws/r", "A", "approval", "B", date("2024-03-15")),
			makeRow(2, "ws/r", "A", "approval", "B", date("2024-01-01")),
			makeRow(3, "ws/r", "A", "approval", "B", date("2024-12-31")),
		}
		m := BuildMeta(rows, nil, nil)
		if m.DateMin != "2024-01-01" {
			t.Errorf("want DateMin=2024-01-01, got %q", m.DateMin)
		}
		if m.DateMax != "2024-12-31" {
			t.Errorf("want DateMax=2024-12-31, got %q", m.DateMax)
		}
	})

	t.Run("repos are sorted", func(t *testing.T) {
		rows := []ActivityRow{
			{SrcRepo: "ws/z-repo", Updated: date("2024-01-01")},
			{SrcRepo: "ws/a-repo", Updated: date("2024-01-01")},
		}
		m := BuildMeta(rows, nil, nil)
		if m.Repos[0] != "a-repo" || m.Repos[1] != "z-repo" {
			t.Errorf("repos not sorted: %v", m.Repos)
		}
	})
}

// --- BuildCharts ---

func TestBuildCharts_Empty(t *testing.T) {
	c := BuildCharts(nil, nil, FilterParams{})
	if c.Summary.TotalPRs != 0 {
		t.Error("expected 0 PRs for empty input")
	}
}

func TestBuildCharts_Summary(t *testing.T) {
	rows := []ActivityRow{
		{ID: 1, SrcRepo: "ws/repo-a", Author: "Alice", Added: 100, Removed: 20, Total: 120, Updated: date("2024-01-01"), Type: "approval", User: "Bob"},
		{ID: 1, SrcRepo: "ws/repo-a", Author: "Alice", Added: 100, Removed: 20, Total: 120, Updated: date("2024-01-01"), Type: "pullrequest_comment", User: "Carol"},
		{ID: 2, SrcRepo: "ws/repo-b", Author: "Dave", Added: 50, Removed: 10, Total: 60, Updated: date("2024-02-01"), Type: "approval", User: "Alice"},
	}
	c := BuildCharts(rows, nil, FilterParams{})

	if c.Summary.TotalPRs != 2 {
		t.Errorf("want TotalPRs=2, got %d", c.Summary.TotalPRs)
	}
	if c.Summary.TotalAdded != 150 {
		t.Errorf("want TotalAdded=150, got %d", c.Summary.TotalAdded)
	}
	if c.Summary.UniqueAuthors != 2 {
		t.Errorf("want UniqueAuthors=2, got %d", c.Summary.UniqueAuthors)
	}
	if c.Summary.UniqueRepos != 2 {
		t.Errorf("want UniqueRepos=2, got %d", c.Summary.UniqueRepos)
	}
}

func TestBuildCharts_PRCountByAuthor(t *testing.T) {
	rows := []ActivityRow{
		{ID: 1, Author: "Alice", SrcRepo: "ws/r", Type: "approval", User: "X", Added: 1, Updated: date("2024-01-01")},
		{ID: 2, Author: "Alice", SrcRepo: "ws/r", Type: "approval", User: "X", Added: 1, Updated: date("2024-01-01")},
		{ID: 3, Author: "Bob", SrcRepo: "ws/r", Type: "approval", User: "X", Added: 1, Updated: date("2024-01-01")},
	}
	c := BuildCharts(rows, nil, FilterParams{})

	byAuthor := map[string]int{}
	for _, lv := range c.PRCountByAuthor {
		byAuthor[lv.Label] = lv.Value
	}
	if byAuthor["Alice"] != 2 || byAuthor["Bob"] != 1 {
		t.Errorf("unexpected PR counts: %v", byAuthor)
	}
}

func TestBuildCharts_ActivityByUser(t *testing.T) {
	rows := []ActivityRow{
		{ID: 1, Author: "A", SrcRepo: "ws/r", Type: "approval", User: "Bob", Updated: date("2024-01-01")},
		{ID: 1, Author: "A", SrcRepo: "ws/r", Type: "pullrequest_comment", User: "Bob", Updated: date("2024-01-01")},
		{ID: 1, Author: "A", SrcRepo: "ws/r", Type: "pullrequest_comment", User: "Carol", Updated: date("2024-01-01")},
	}
	c := BuildCharts(rows, nil, FilterParams{})

	byUser := map[string]ActivityBreakdown{}
	for _, ab := range c.ActivityByUser {
		byUser[ab.Label] = ab
	}
	if byUser["Bob"].Approval != 1 || byUser["Bob"].Comment != 1 {
		t.Errorf("Bob: want approval=1 comment=1, got %+v", byUser["Bob"])
	}
	if byUser["Carol"].Comment != 1 {
		t.Errorf("Carol: want comment=1, got %+v", byUser["Carol"])
	}
}

func TestBuildCharts_PRCountByMonth(t *testing.T) {
	rows := []ActivityRow{
		{ID: 1, Author: "A", SrcRepo: "ws/r", Type: "approval", User: "X", Updated: date("2024-01-15"), Added: 1},
		{ID: 2, Author: "A", SrcRepo: "ws/r", Type: "approval", User: "X", Updated: date("2024-01-20"), Added: 1},
		{ID: 3, Author: "A", SrcRepo: "ws/r", Type: "approval", User: "X", Updated: date("2024-02-10"), Added: 1},
	}
	c := BuildCharts(rows, nil, FilterParams{})

	byMonth := map[string]int{}
	for _, lv := range c.PRCountByMonth {
		byMonth[lv.Label] = lv.Value
	}
	if byMonth["2024-01"] != 2 || byMonth["2024-02"] != 1 {
		t.Errorf("unexpected month counts: %v", byMonth)
	}
}

func TestBuildCharts_SortOrder(t *testing.T) {
	rows := []ActivityRow{
		{ID: 1, Author: "Bob", SrcRepo: "ws/r", Type: "approval", User: "X", Added: 5, Total: 5, Updated: date("2024-01-01")},
		{ID: 2, Author: "Alice", SrcRepo: "ws/r", Type: "approval", User: "X", Added: 100, Total: 100, Updated: date("2024-01-01")},
		{ID: 3, Author: "Alice", SrcRepo: "ws/r", Type: "approval", User: "X", Added: 100, Total: 100, Updated: date("2024-01-01")},
	}
	c := BuildCharts(rows, nil, FilterParams{})

	// PRCountByAuthor: Alice(2) before Bob(1)
	if c.PRCountByAuthor[0].Label != "Alice" {
		t.Errorf("want Alice first in PRCountByAuthor, got %q", c.PRCountByAuthor[0].Label)
	}
	// CodeChangesByAuthor: Alice(200 total) before Bob(5 total)
	if c.CodeChangesByAuthor[0].Label != "Alice" {
		t.Errorf("want Alice first in CodeChangesByAuthor, got %q", c.CodeChangesByAuthor[0].Label)
	}
}
