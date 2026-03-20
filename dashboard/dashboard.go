package dashboard

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// ActivityRow mirrors one row in the pr_report table.
type ActivityRow struct {
	ID          int
	SrcRepo     string
	SrcBranch   string
	DestRepo    string
	DestBranch  string
	Title       string
	Description string
	State       string
	Author      string
	Created     time.Time
	Updated     time.Time
	FileChanged int
	Added       int
	Removed     int
	Total       int
	Type        string // "approval" or "pullrequest_comment"
	User        string
	Content     string
}

// PRRow is the deduplicated PR view (one entry per unique PR ID).
type PRRow struct {
	ID          int    `json:"id"`
	SrcRepo     string `json:"srcRepo"`
	SrcBranch   string `json:"srcBranch"`
	DestRepo    string `json:"destRepo"`
	DestBranch  string `json:"destBranch"`
	Title       string `json:"title"`
	State       string `json:"state"`
	Author      string `json:"author"`
	Created     string `json:"created"`
	Updated     string `json:"updated"`
	FileChanged int    `json:"fileChanged"`
	Added       int    `json:"added"`
	Removed     int    `json:"removed"`
	Total       int    `json:"total"`
}

// FilterParams carries parsed query parameters.
type FilterParams struct {
	DateFrom     time.Time
	DateTo       time.Time
	Repos        []string
	Authors      []string // include filter: matches Author OR User
	ExcludeUsers []string // exclude filter: drops rows where Author OR User matches
}

// DashboardStore holds in-memory data loaded from the pr_report DB table.
type DashboardStore struct {
	mu              sync.RWMutex
	activities      []ActivityRow
	dsn             string
	botSet          map[string]bool
	excludedAuthors []string
}

func NewStore(dsn string, excludedAuthors []string) *DashboardStore {
	return &DashboardStore{dsn: dsn, botSet: toSet(excludedAuthors), excludedAuthors: excludedAuthors}
}

// BotSet returns the set of excluded author/user names.
func (s *DashboardStore) BotSet() map[string]bool { return s.botSet }

// ExcludedAuthors returns the configured excluded author/user names.
func (s *DashboardStore) ExcludedAuthors() []string { return s.excludedAuthors }

// Load queries the pr_report table and refreshes the in-memory cache.
func (s *DashboardStore) Load() error {
	db, err := sql.Open("postgres", s.dsn)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT pr_id, src_repo, src_branch, dest_repo, dest_branch,
		       title, description, state, author, created, updated,
		       file_changed, added, removed, total,
		       activity_type, activity_user, activity_content
		FROM pr_report
		ORDER BY pr_id
	`)
	if err != nil {
		return fmt.Errorf("query pr_report: %w", err)
	}
	defer rows.Close()

	var result []ActivityRow
	for rows.Next() {
		var r ActivityRow
		var created, updated string
		if err := rows.Scan(
			&r.ID, &r.SrcRepo, &r.SrcBranch, &r.DestRepo, &r.DestBranch,
			&r.Title, &r.Description, &r.State, &r.Author, &created, &updated,
			&r.FileChanged, &r.Added, &r.Removed, &r.Total,
			&r.Type, &r.User, &r.Content,
		); err != nil {
			return err
		}
		r.Created, _ = time.Parse("2006-01-02", created)
		r.Updated, _ = time.Parse("2006-01-02", updated)
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	s.activities = result
	s.mu.Unlock()
	return nil
}

func (s *DashboardStore) Activities() []ActivityRow {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activities
}

func (s *DashboardStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.activities)
}

// shortRepo strips the workspace prefix from "workspace/repo-name".
func shortRepo(fullName string) string {
	parts := strings.SplitN(fullName, "/", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return fullName
}

func filterActivities(rows []ActivityRow, p FilterParams, bots map[string]bool) []ActivityRow {
	repoSet      := toSet(p.Repos)
	authorSet    := toSet(p.Authors)
	excludeSet   := toSet(p.ExcludeUsers)

	var out []ActivityRow
	for _, r := range rows {
		if bots[r.Author] || bots[r.User] {
			continue
		}
		if len(excludeSet) > 0 && (excludeSet[r.Author] || excludeSet[r.User]) {
			continue
		}
		if !p.DateFrom.IsZero() && r.Updated.Before(p.DateFrom) {
			continue
		}
		if !p.DateTo.IsZero() && r.Updated.After(p.DateTo) {
			continue
		}
		if len(repoSet) > 0 && !repoSet[shortRepo(r.SrcRepo)] {
			continue
		}
		// Authors filter matches on either PR author or activity user.
		if len(authorSet) > 0 && !authorSet[r.Author] && !authorSet[r.User] {
			continue
		}
		out = append(out, r)
	}
	return out
}

func toSet(items []string) map[string]bool {
	if len(items) == 0 {
		return nil
	}
	s := make(map[string]bool, len(items))
	for _, v := range items {
		if v != "" {
			s[v] = true
		}
	}
	return s
}

// deduplicatePRs returns one PRRow per unique PR ID (first occurrence wins).
// Code-change totals are pre-aggregated per-PR in pr_report, so first row is correct.
func deduplicatePRs(rows []ActivityRow) []PRRow {
	seen := make(map[int]bool)
	var prs []PRRow
	for _, r := range rows {
		if seen[r.ID] {
			continue
		}
		seen[r.ID] = true
		prs = append(prs, PRRow{
			ID:          r.ID,
			SrcRepo:     shortRepo(r.SrcRepo),
			SrcBranch:   r.SrcBranch,
			DestRepo:    shortRepo(r.DestRepo),
			DestBranch:  r.DestBranch,
			Title:       r.Title,
			State:       r.State,
			Author:      r.Author,
			Created:     r.Created.Format("2006-01-02"),
			Updated:     r.Updated.Format("2006-01-02"),
			FileChanged: r.FileChanged,
			Added:       r.Added,
			Removed:     r.Removed,
			Total:       r.Total,
		})
	}
	return prs
}

// --- Aggregation types ---

type LabelValue struct {
	Label string `json:"label"`
	Value int    `json:"value"`
}

type CodeChanges struct {
	Label   string `json:"label"`
	Added   int    `json:"added"`
	Removed int    `json:"removed"`
	Total   int    `json:"total"`
}

type ActivityBreakdown struct {
	Label    string `json:"label"`
	Approval int    `json:"approval"`
	Comment  int    `json:"comment"`
}

type Summary struct {
	TotalPRs      int `json:"totalPRs"`
	TotalAdded    int `json:"totalAdded"`
	TotalRemoved  int `json:"totalRemoved"`
	TotalChanged  int `json:"totalChanged"`
	UniqueAuthors int `json:"uniqueAuthors"`
	UniqueRepos   int `json:"uniqueRepos"`
}

type ChartsResponse struct {
	Summary             Summary             `json:"summary"`
	PRCountByAuthor     []LabelValue        `json:"prCountByAuthor"`
	CodeChangesByAuthor []CodeChanges       `json:"codeChangesByAuthor"`
	ActivityByUser      []ActivityBreakdown `json:"activityByUser"`
	PRCountByMonth      []LabelValue        `json:"prCountByMonth"`
	CodeChangesByRepo   []CodeChanges       `json:"codeChangesByRepo"`
}

// BuildCharts aggregates filtered activity rows into chart data.
// bots is an optional set of excluded user names; pass nil to include everyone.
// p controls per-chart field-specific filtering:
//   - PR Count / Code Changes / Summary / PR by Month / Changes by Repo →
//     Authors and ExcludeUsers are matched against the Author field only.
//   - Review Activity by User →
//     Authors and ExcludeUsers are matched against the User field only.
func BuildCharts(rows []ActivityRow, bots map[string]bool, p FilterParams) ChartsResponse {
	authorSet  := toSet(p.Authors)
	excludeSet := toSet(p.ExcludeUsers)

	// prRows: rows whose PR Author passes the include/exclude filters.
	var prRows []ActivityRow
	for _, r := range rows {
		if len(authorSet) > 0 && !authorSet[r.Author] {
			continue
		}
		if excludeSet[r.Author] {
			continue
		}
		prRows = append(prRows, r)
	}

	prs := deduplicatePRs(prRows)

	authors := make(map[string]bool)
	repos := make(map[string]bool)
	totalAdded, totalRemoved, totalChanged := 0, 0, 0
	for _, pr := range prs {
		authors[pr.Author] = true
		repos[pr.SrcRepo] = true
		totalAdded += pr.Added
		totalRemoved += pr.Removed
		totalChanged += pr.Total
	}

	prByAuthor := make(map[string]int)
	for _, pr := range prs {
		prByAuthor[pr.Author]++
	}

	codeByAuthor := make(map[string]*CodeChanges)
	for _, pr := range prs {
		if _, ok := codeByAuthor[pr.Author]; !ok {
			codeByAuthor[pr.Author] = &CodeChanges{Label: pr.Author}
		}
		codeByAuthor[pr.Author].Added += pr.Added
		codeByAuthor[pr.Author].Removed += pr.Removed
		codeByAuthor[pr.Author].Total += pr.Total
	}

	// actByUser: rows whose activity User passes the include/exclude filters.
	actByUser := make(map[string]*ActivityBreakdown)
	for _, r := range rows {
		if r.User == "" || bots[r.User] {
			continue
		}
		if len(authorSet) > 0 && !authorSet[r.User] {
			continue
		}
		if excludeSet[r.User] {
			continue
		}
		if _, ok := actByUser[r.User]; !ok {
			actByUser[r.User] = &ActivityBreakdown{Label: r.User}
		}
		if r.Type == "approval" {
			actByUser[r.User].Approval++
		} else {
			actByUser[r.User].Comment++
		}
	}

	prByMonth := make(map[string]int)
	for _, pr := range prs {
		if len(pr.Updated) >= 7 {
			prByMonth[pr.Updated[:7]]++
		}
	}

	codeByRepo := make(map[string]*CodeChanges)
	for _, pr := range prs {
		if _, ok := codeByRepo[pr.SrcRepo]; !ok {
			codeByRepo[pr.SrcRepo] = &CodeChanges{Label: pr.SrcRepo}
		}
		codeByRepo[pr.SrcRepo].Added += pr.Added
		codeByRepo[pr.SrcRepo].Removed += pr.Removed
		codeByRepo[pr.SrcRepo].Total += pr.Total
	}

	return ChartsResponse{
		Summary: Summary{
			TotalPRs:      len(prs),
			TotalAdded:    totalAdded,
			TotalRemoved:  totalRemoved,
			TotalChanged:  totalChanged,
			UniqueAuthors: len(authors),
			UniqueRepos:   len(repos),
		},
		PRCountByAuthor:     sortedByValue(mapToLabelValues(prByAuthor)),
		CodeChangesByAuthor: sortedCodeChanges(mapValuesToSlice(codeByAuthor)),
		ActivityByUser:      sortedActivity(mapActivityToSlice(actByUser)),
		PRCountByMonth:      sortedByLabel(mapToLabelValues(prByMonth)),
		CodeChangesByRepo:   sortedCodeChanges(mapValuesToSlice(codeByRepo)),
	}
}

// MetaResponse is returned by /api/meta for populating filter dropdowns.
type MetaResponse struct {
	Repos           []string `json:"repos"`
	Authors         []string `json:"authors"`
	Users           []string `json:"users"`
	ExcludedAuthors []string `json:"excludedAuthors"`
	DateMin         string   `json:"dateMin"`
	DateMax         string   `json:"dateMax"`
}

// BuildMeta scans all rows to find distinct filter options and date bounds.
// bots is an optional set of excluded user names; pass nil to include everyone.
func BuildMeta(rows []ActivityRow, bots map[string]bool, excludedAuthors []string) MetaResponse {
	repoSet := make(map[string]bool)
	authorSet := make(map[string]bool)
	userSet := make(map[string]bool)
	var minDate, maxDate time.Time

	for _, r := range rows {
		repoSet[shortRepo(r.SrcRepo)] = true
		if r.Author != "" && !bots[r.Author] {
			authorSet[r.Author] = true
		}
		if r.User != "" && !bots[r.User] {
			userSet[r.User] = true
		}
		if !r.Updated.IsZero() {
			if minDate.IsZero() || r.Updated.Before(minDate) {
				minDate = r.Updated
			}
			if r.Updated.After(maxDate) {
				maxDate = r.Updated
			}
		}
	}

	dateMin, dateMax := "", ""
	if !minDate.IsZero() {
		dateMin = minDate.Format("2006-01-02")
	}
	if !maxDate.IsZero() {
		dateMax = maxDate.Format("2006-01-02")
	}

	return MetaResponse{
		Repos:           sortedKeys(repoSet),
		Authors:         sortedKeys(authorSet),
		Users:           sortedKeys(userSet),
		ExcludedAuthors: excludedAuthors,
		DateMin:         dateMin,
		DateMax:         dateMax,
	}
}

// --- helpers ---

func mapToLabelValues(m map[string]int) []LabelValue {
	out := make([]LabelValue, 0, len(m))
	for k, v := range m {
		out = append(out, LabelValue{Label: k, Value: v})
	}
	return out
}

func sortedByValue(s []LabelValue) []LabelValue {
	sort.Slice(s, func(i, j int) bool { return s[i].Value > s[j].Value })
	return s
}

func sortedByLabel(s []LabelValue) []LabelValue {
	sort.Slice(s, func(i, j int) bool { return s[i].Label < s[j].Label })
	return s
}

func mapValuesToSlice(m map[string]*CodeChanges) []CodeChanges {
	out := make([]CodeChanges, 0, len(m))
	for _, v := range m {
		out = append(out, *v)
	}
	return out
}

func sortedCodeChanges(s []CodeChanges) []CodeChanges {
	sort.Slice(s, func(i, j int) bool { return s[i].Total > s[j].Total })
	return s
}

func mapActivityToSlice(m map[string]*ActivityBreakdown) []ActivityBreakdown {
	out := make([]ActivityBreakdown, 0, len(m))
	for _, v := range m {
		out = append(out, *v)
	}
	return out
}

func sortedActivity(s []ActivityBreakdown) []ActivityBreakdown {
	sort.Slice(s, func(i, j int) bool {
		return (s[i].Approval + s[i].Comment) > (s[j].Approval + s[j].Comment)
	})
	return s
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
