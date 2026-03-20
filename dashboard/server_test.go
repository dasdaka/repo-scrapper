package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// stubStore builds a DashboardStore pre-loaded with the given rows.
func stubStore(rows []ActivityRow) *DashboardStore {
	s := &DashboardStore{}
	s.activities = rows
	return s
}

func sampleRows() []ActivityRow {
	return []ActivityRow{
		{
			ID: 1, SrcRepo: "ws/repo-a", SrcBranch: "feature/x",
			DestRepo: "ws/repo-a", DestBranch: "main",
			Title: "PR One", State: "MERGED", Author: "Alice",
			Type: "approval", User: "Bob",
			Added: 50, Removed: 10, Total: 60,
			Created: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			Updated: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			ID: 1, SrcRepo: "ws/repo-a", SrcBranch: "feature/x",
			DestRepo: "ws/repo-a", DestBranch: "main",
			Title: "PR One", State: "MERGED", Author: "Alice",
			Type: "pullrequest_comment", User: "Carol",
			Added: 50, Removed: 10, Total: 60,
			Created: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			Updated: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			ID: 2, SrcRepo: "ws/repo-b", SrcBranch: "fix/y",
			DestRepo: "ws/repo-b", DestBranch: "main",
			Title: "PR Two", State: "OPEN", Author: "Dave",
			Type: "approval", User: "Alice",
			Added: 20, Removed: 5, Total: 25,
			Created: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			Updated: time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC),
		},
	}
}

// --- /api/meta ---

func TestMetaHandler(t *testing.T) {
	store := stubStore(sampleRows())

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/meta", nil)
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, BuildMeta(store.Activities(), nil, nil))
	}).ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("want 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("want application/json, got %q", ct)
	}

	var meta MetaResponse
	if err := json.NewDecoder(w.Body).Decode(&meta); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(meta.Repos) != 2 {
		t.Errorf("want 2 repos, got %d: %v", len(meta.Repos), meta.Repos)
	}
	if len(meta.Authors) != 2 {
		t.Errorf("want 2 authors, got %d", len(meta.Authors))
	}
	if meta.DateMin != "2024-01-15" {
		t.Errorf("want DateMin=2024-01-15, got %q", meta.DateMin)
	}
	if meta.DateMax != "2024-02-20" {
		t.Errorf("want DateMax=2024-02-20, got %q", meta.DateMax)
	}
}

// --- /api/charts ---

func TestChartsHandler(t *testing.T) {
	store := stubStore(sampleRows())

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/charts", nil)
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := parseFilters(r)
		writeJSON(w, BuildCharts(filterActivities(store.Activities(), params, nil), nil, params))
	}).ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("want 200, got %d", w.Code)
	}

	var charts ChartsResponse
	if err := json.NewDecoder(w.Body).Decode(&charts); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if charts.Summary.TotalPRs != 2 {
		t.Errorf("want TotalPRs=2, got %d", charts.Summary.TotalPRs)
	}
}

func TestChartsHandler_WithRepoFilter(t *testing.T) {
	store := stubStore(sampleRows())

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/charts?repos=repo-a", nil)
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := parseFilters(r)
		writeJSON(w, BuildCharts(filterActivities(store.Activities(), params, nil), nil, params))
	}).ServeHTTP(w, r)

	var charts ChartsResponse
	json.NewDecoder(w.Body).Decode(&charts)
	if charts.Summary.TotalPRs != 1 {
		t.Errorf("repo filter: want 1 PR, got %d", charts.Summary.TotalPRs)
	}
}

// --- /api/table ---

func TestTableHandler_Pagination(t *testing.T) {
	// Build 5 distinct PRs (each with one activity row).
	rows := make([]ActivityRow, 5)
	for i := range rows {
		rows[i] = ActivityRow{
			ID: i + 1, SrcRepo: "ws/r", Author: "A",
			Type: "approval", User: "B",
			Updated: time.Date(2024, 1, i+1, 0, 0, 0, 0, time.UTC),
		}
	}
	store := stubStore(rows)

	cases := []struct {
		url           string
		wantTotal     int
		wantPage      int
		wantDataCount int
	}{
		{"/api/table", 5, 1, 5},
		{"/api/table?page=1&pageSize=2", 5, 1, 2},
		{"/api/table?page=2&pageSize=2", 5, 2, 2},
		{"/api/table?page=3&pageSize=2", 5, 3, 1},
		{"/api/table?page=99&pageSize=2", 5, 99, 0},
	}

	for _, tc := range cases {
		t.Run(tc.url, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, tc.url, nil)
			tableHandler(store).ServeHTTP(w, r)

			if w.Code != http.StatusOK {
				t.Errorf("want 200, got %d", w.Code)
			}

			var resp struct {
				Total    int      `json:"total"`
				Page     int      `json:"page"`
				PageSize int      `json:"pageSize"`
				Data     []PRRow  `json:"data"`
			}
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if resp.Total != tc.wantTotal {
				t.Errorf("total: want %d, got %d", tc.wantTotal, resp.Total)
			}
			if resp.Page != tc.wantPage {
				t.Errorf("page: want %d, got %d", tc.wantPage, resp.Page)
			}
			if len(resp.Data) != tc.wantDataCount {
				t.Errorf("data count: want %d, got %d", tc.wantDataCount, len(resp.Data))
			}
		})
	}
}

func TestTableHandler_Filter(t *testing.T) {
	store := stubStore(sampleRows())

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/table?repos=repo-b", nil)
	tableHandler(store).ServeHTTP(w, r)

	var resp struct {
		Total int     `json:"total"`
		Data  []PRRow `json:"data"`
	}
	json.NewDecoder(w.Body).Decode(&resp)

	if resp.Total != 1 {
		t.Errorf("want 1 PR for repo-b, got %d", resp.Total)
	}
	if len(resp.Data) != 1 || resp.Data[0].SrcRepo != "repo-b" {
		t.Errorf("unexpected data: %+v", resp.Data)
	}
}

// --- parseFilters ---

func TestParseFilters(t *testing.T) {
	t.Run("empty query returns zero FilterParams", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		p := parseFilters(r)
		if !p.DateFrom.IsZero() || !p.DateTo.IsZero() {
			t.Error("expected zero times")
		}
		if len(p.Repos) != 0 || len(p.Authors) != 0 {
			t.Error("expected empty slices")
		}
	})

	t.Run("parses dateFrom and dateTo", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/?dateFrom=2024-01-01&dateTo=2024-12-31", nil)
		p := parseFilters(r)
		if p.DateFrom.Year() != 2024 || p.DateFrom.Month() != 1 {
			t.Errorf("unexpected DateFrom: %v", p.DateFrom)
		}
		if p.DateTo.Year() != 2024 || p.DateTo.Month() != 12 {
			t.Errorf("unexpected DateTo: %v", p.DateTo)
		}
	})

	t.Run("parses multiple repos", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/?repos=repo-a&repos=repo-b", nil)
		p := parseFilters(r)
		if len(p.Repos) != 2 {
			t.Errorf("want 2 repos, got %d", len(p.Repos))
		}
	})

	t.Run("blank values are filtered out", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/?repos=&repos=repo-a", nil)
		p := parseFilters(r)
		if len(p.Repos) != 1 || p.Repos[0] != "repo-a" {
			t.Errorf("want [repo-a], got %v", p.Repos)
		}
	})
}

// --- filterEmpty ---

func TestFilterEmpty(t *testing.T) {
	cases := []struct {
		input []string
		want  int
	}{
		{nil, 0},
		{[]string{}, 0},
		{[]string{"", "  ", "a"}, 1},
		{[]string{"a", "b"}, 2},
	}
	for _, tc := range cases {
		got := filterEmpty(tc.input)
		if len(got) != tc.want {
			t.Errorf("filterEmpty(%v): want %d, got %d", tc.input, tc.want, len(got))
		}
	}
}
