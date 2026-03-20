package util

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// defaultPagelen is the page size sent on every Bitbucket API request.
// 50 is the maximum value accepted by most Bitbucket Cloud endpoints.
const defaultPagelen = 50

// --- HTTP doer interface ---

// HTTPDoer is satisfied by *http.Client and allows injection in tests.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// --- retry & concurrency config ---

// RetryConfig controls exponential-backoff retry behaviour.
type RetryConfig struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

var defaultRetry = RetryConfig{
	MaxAttempts: 4,
	BaseBackoff: time.Second,
	MaxBackoff:  30 * time.Second,
}

type semaphore chan struct{}

func newSemaphore(n int) semaphore { return make(chan struct{}, n) }
func (s semaphore) acquire()       { s <- struct{}{} }
func (s semaphore) release()       { <-s }

// --- client ---

// Client fetches pull request data from the Bitbucket Cloud API.
type Client struct {
	cfg   BitbucketConfig
	http  HTTPDoer
	sem   semaphore
	retry RetryConfig
	logf  LogFunc
}

// NewClient returns a Client with sensible defaults.
func NewClient(cfg BitbucketConfig) *Client {
	return &Client{
		cfg:   cfg,
		http:  &http.Client{Timeout: 30 * time.Second},
		sem:   newSemaphore(5),
		retry: defaultRetry,
		logf:  TerminalLog,
	}
}

// WithHTTPDoer replaces the HTTP client (used in tests).
func WithHTTPDoer(h HTTPDoer) func(*Client) {
	return func(c *Client) { c.http = h }
}

// WithConcurrency sets the maximum number of concurrent per-PR goroutines.
func WithConcurrency(n int) func(*Client) {
	return func(c *Client) { c.sem = newSemaphore(n) }
}

// WithRetry overrides the retry configuration.
func WithRetry(r RetryConfig) func(*Client) {
	return func(c *Client) { c.retry = r }
}

// WithLogger sets the LogFunc used for progress and status messages.
// Defaults to TerminalLog (writes to the standard Go logger).
func WithLogger(logf LogFunc) func(*Client) {
	return func(c *Client) { c.logf = logf }
}

// NewClientWithOptions creates a Client with the given options applied.
func NewClientWithOptions(cfg BitbucketConfig, opts ...func(*Client)) *Client {
	c := NewClient(cfg)
	for _, o := range opts {
		o(c)
	}
	return c
}

// --- query & URL helpers ---

// buildQueryFilter computes the Bitbucket query string from the config.
// ScrapePeriod generates a date filter; QueryFilter appends extra clauses.
func buildQueryFilter(cfg BitbucketConfig) string {
	var parts []string

	switch strings.ToLower(cfg.ScrapePeriod) {
	case "daily":
		since := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
		parts = append(parts, fmt.Sprintf(`updated_on >= "%s"`, since))
	case "monthly":
		t := time.Now().UTC()
		since := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
		parts = append(parts, fmt.Sprintf(`updated_on >= "%s"`, since))
	}

	if cfg.QueryFilter != "" {
		parts = append(parts, cfg.QueryFilter)
	}

	return strings.Join(parts, " AND ")
}

// buildFirstURL returns the first-page URL for the PR list of the given repo.
// When PullRequestURL is configured it is used as a template (three %s: workspace,
// repo, query); otherwise the default Bitbucket Cloud endpoint is used.
func (c *Client) buildFirstURL(repo string) string {
	q := url.QueryEscape(buildQueryFilter(c.cfg))
	if c.cfg.PullRequestURL != "" {
		return fmt.Sprintf(c.cfg.PullRequestURL, c.cfg.Workspace, repo, q)
	}
	return fmt.Sprintf(
		"https://api.bitbucket.org/2.0/repositories/%s/%s/pullrequests?pagelen=%d&sort=-updated_on&q=%s",
		url.PathEscape(c.cfg.Workspace), url.PathEscape(repo), defaultPagelen, q,
	)
}

// --- http execution ---

// do executes a request, retrying on transport errors and HTTP 429.
// It respects Retry-After and X-RateLimit-NearLimit headers.
func (c *Client) do(ctx context.Context, makeReq func() (*http.Request, error)) (*http.Response, error) {
	backoff := c.retry.BaseBackoff
	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		req, err := makeReq()
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}

		resp, err := c.http.Do(req)
		if err != nil {
			if attempt == c.retry.MaxAttempts {
				return nil, err
			}
			if sleepErr := ctxSleep(ctx, jitter(backoff)); sleepErr != nil {
				return nil, sleepErr
			}
			backoff = minDuration(backoff*2, c.retry.MaxBackoff)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := parseRetryAfter(resp.Header.Get("Retry-After"))
			if wait == 0 {
				wait = backoff
			}
			resp.Body.Close()
			if attempt == c.retry.MaxAttempts {
				return nil, fmt.Errorf("rate limited after %d attempts", c.retry.MaxAttempts)
			}
			c.logf("rate limited; sleeping %s (attempt %d/%d)", wait, attempt, c.retry.MaxAttempts)
			if sleepErr := ctxSleep(ctx, wait); sleepErr != nil {
				return nil, sleepErr
			}
			backoff = minDuration(backoff*2, c.retry.MaxBackoff)
			continue
		}

		// Near the rate limit — add a small proactive delay before returning.
		if resp.Header.Get("X-RateLimit-NearLimit") == "1" {
			c.logf("near rate limit; throttling")
			if sleepErr := ctxSleep(ctx, jitter(c.retry.BaseBackoff)); sleepErr != nil {
				resp.Body.Close()
				return nil, sleepErr
			}
		}

		return resp, nil
	}
	return nil, fmt.Errorf("exceeded %d retry attempts", c.retry.MaxAttempts)
}

// getJSON GETs url, decodes the JSON body into dst, honouring retry logic.
func (c *Client) getJSON(ctx context.Context, url string, dst interface{}) error {
	resp, err := c.do(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", c.cfg.Token)
		return req, nil
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	return json.Unmarshal(body, dst)
}

// getRaw GETs url and returns the raw response body, honouring retry logic.
func (c *Client) getRaw(ctx context.Context, rawURL string) ([]byte, error) {
	resp, err := c.do(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", c.cfg.Token)
		return req, nil
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// ctxSleep sleeps for d or until ctx is done.
func ctxSleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// jitter adds up to 20 % random noise to d to spread retry storms.
func jitter(d time.Duration) time.Duration {
	return d + time.Duration(rand.Int63n(int64(d/5)+1))
}

// minDuration returns the smaller of a and b.
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// parseRetryAfter parses an HTTP Retry-After header value (seconds or HTTP-date).
func parseRetryAfter(s string) time.Duration {
	if s == "" {
		return 0
	}
	if secs, err := strconv.Atoi(s); err == nil {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(s); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// --- generic paginator ---

// pageResponse is the common shape of all Bitbucket paginated responses.
type pageResponse[T any] struct {
	Values []T    `json:"values"`
	Next   string `json:"next"`
}

// fetchAllPages fetches every page starting from firstURL by following "next" links.
func fetchAllPages[T any](ctx context.Context, c *Client, firstURL string) ([]T, error) {
	var all []T
	url := firstURL
	for url != "" {
		var p pageResponse[T]
		if err := c.getJSON(ctx, url, &p); err != nil {
			return nil, err
		}
		all = append(all, p.Values...)
		url = p.Next
	}
	return all, nil
}

// --- per-PR bundle ---

// prSubData collects every piece of data scraped for a single pull request.
type prSubData struct {
	pr       PullRequestData
	commits  []CommitData
	activity []PullRequestActivityData
	diffstat []DiffStatActivityData
	comments []CommentData
	statuses []BuildStatus
}

// --- public scraping API ---

// ScrapeRaw fetches all raw PR data from the Bitbucket API for every configured
// repo and stores it in the raw DB tables. It does not aggregate or export.
func (c *Client) ScrapeRaw(ctx context.Context, db *sql.DB) error {
	for _, repo := range c.cfg.RepoList {
		c.logf("[%s] scraping raw data", repo)
		if err := c.scrapeRawData(ctx, db, repo); err != nil {
			return fmt.Errorf("repo %s: %w", repo, err)
		}
		c.logf("[%s] raw data stored", repo)
	}
	return nil
}

// --- internal scraping ---

func (c *Client) scrapeRawData(ctx context.Context, db *sql.DB, repo string) error {
	c.logf("[%s] fetching pull requests (period=%q)", repo, c.cfg.ScrapePeriod)
	prs, err := fetchAllPages[PullRequestData](ctx, c, c.buildFirstURL(repo))
	if err != nil {
		return fmt.Errorf("fetch PR list: %w", err)
	}
	c.logf("[%s] %d pull requests fetched", repo, len(prs))

	if err := UpsertPullRequests(ctx, db, repo, prs); err != nil {
		return fmt.Errorf("store pull_requests: %w", err)
	}

	// Fetch all per-PR sub-data concurrently, bounded by the semaphore.
	type result struct {
		data prSubData
		err  error
	}
	results := make([]result, len(prs))
	var wg sync.WaitGroup
	for i, pr := range prs {
		wg.Add(1)
		go func(idx int, pr PullRequestData) {
			defer wg.Done()
			c.sem.acquire()
			defer c.sem.release()
			data, err := c.fetchAllPRData(ctx, pr)
			results[idx] = result{data, err}
		}(i, pr)
	}
	wg.Wait()

	var (
		allCommits  []CommitData
		allActivity []PullRequestActivityData
		allDiffstat []DiffStatActivityData
		allComments []CommentData
		allStatuses []BuildStatus
	)
	for _, r := range results {
		if r.err != nil {
			return fmt.Errorf("fetch PR sub-data: %w", r.err)
		}
		allCommits = append(allCommits, r.data.commits...)
		allActivity = append(allActivity, r.data.activity...)
		allDiffstat = append(allDiffstat, r.data.diffstat...)
		allComments = append(allComments, r.data.comments...)
		allStatuses = append(allStatuses, r.data.statuses...)
	}
	c.logf("[%s] %d commits, %d activity, %d diffstat, %d comments, %d statuses",
		repo, len(allCommits), len(allActivity), len(allDiffstat), len(allComments), len(allStatuses))

	if err := UpsertPRCommits(ctx, db, repo, allCommits); err != nil {
		return fmt.Errorf("store pr_commits: %w", err)
	}
	if err := UpsertPRActivity(ctx, db, repo, allActivity); err != nil {
		return fmt.Errorf("store pr_activity: %w", err)
	}
	if err := UpsertPRDiffStat(ctx, db, repo, allDiffstat); err != nil {
		return fmt.Errorf("store pr_diffstat: %w", err)
	}
	if err := UpsertPRComments(ctx, db, repo, allComments); err != nil {
		return fmt.Errorf("store pr_comments: %w", err)
	}
	if err := UpsertPRStatuses(ctx, db, repo, allStatuses); err != nil {
		return fmt.Errorf("store pr_statuses: %w", err)
	}
	return nil
}

// fetchAllPRData sequentially scrapes every sub-resource linked from pr.
func (c *Client) fetchAllPRData(ctx context.Context, pr PullRequestData) (prSubData, error) {
	data := prSubData{pr: pr}
	var err error
	if data.commits, err = c.fetchAllCommits(ctx, pr); err != nil {
		return data, fmt.Errorf("PR %d commits: %w", pr.ID, err)
	}
	if data.activity, err = c.fetchAllActivity(ctx, pr); err != nil {
		return data, fmt.Errorf("PR %d activity: %w", pr.ID, err)
	}
	if data.diffstat, err = c.fetchDiffStat(ctx, pr); err != nil {
		return data, fmt.Errorf("PR %d diffstat: %w", pr.ID, err)
	}
	if data.comments, err = c.fetchAllComments(ctx, pr); err != nil {
		return data, fmt.Errorf("PR %d comments: %w", pr.ID, err)
	}
	if data.statuses, err = c.fetchAllStatuses(ctx, pr); err != nil {
		return data, fmt.Errorf("PR %d statuses: %w", pr.ID, err)
	}
	return data, nil
}

func (c *Client) fetchAllCommits(ctx context.Context, pr PullRequestData) ([]CommitData, error) {
	if pr.Links.Commits.Href == "" {
		return nil, nil
	}
	return fetchAllPages[CommitData](ctx, c, pr.Links.Commits.Href)
}

func (c *Client) fetchAllActivity(ctx context.Context, pr PullRequestData) ([]PullRequestActivityData, error) {
	if pr.Links.Activity.Href == "" {
		return nil, nil
	}
	firstURL := fmt.Sprintf("%s?pagelen=%d", pr.Links.Activity.Href, defaultPagelen)
	data, err := fetchAllPages[PullRequestActivityData](ctx, c, firstURL)
	if err != nil {
		return nil, err
	}
	for i := range data {
		if data[i].PullRequest.ID == 0 {
			data[i].PullRequest.ID = pr.ID
		}
	}
	return data, nil
}

// fetchDiffStat fetches the raw unified diff for a PR from the /diff endpoint
// and counts added/removed lines by parsing the diff text. This works for repos
// where the /diffstat JSON endpoint returns no data (e.g. some server configs).
func (c *Client) fetchDiffStat(ctx context.Context, pr PullRequestData) ([]DiffStatActivityData, error) {
	diffURL := pr.Links.Diff.Href
	if diffURL == "" {
		diffURL = fmt.Sprintf(
			"https://api.bitbucket.org/2.0/repositories/%s/%s/pullrequests/%d/diff",
			url.PathEscape(c.cfg.Workspace),
			url.PathEscape(pr.Destination.Repository.Name),
			pr.ID,
		)
	}
	body, err := c.getRaw(ctx, diffURL)
	if err != nil {
		return nil, err
	}
	added, removed := parseDiff(body)
	return []DiffStatActivityData{{
		PullRequestID: pr.ID,
		LinesAdded:    added,
		LinesRemoved:  removed,
	}}, nil
}

// parseDiff counts added and removed lines in a unified diff.
// Lines beginning with '+' (but not '+++') are added; '-' (but not '---') are removed.
func parseDiff(diff []byte) (added, removed int) {
	for _, line := range bytes.Split(diff, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		switch {
		case line[0] == '+' && !bytes.HasPrefix(line, []byte("+++")):
			added++
		case line[0] == '-' && !bytes.HasPrefix(line, []byte("---")):
			removed++
		}
	}
	return
}

func (c *Client) fetchAllComments(ctx context.Context, pr PullRequestData) ([]CommentData, error) {
	if pr.Links.Comments.Href == "" {
		return nil, nil
	}
	data, err := fetchAllPages[CommentData](ctx, c, pr.Links.Comments.Href)
	if err != nil {
		return nil, err
	}
	for i := range data {
		data[i].PullRequestID = pr.ID
	}
	return data, nil
}

func (c *Client) fetchAllStatuses(ctx context.Context, pr PullRequestData) ([]BuildStatus, error) {
	if pr.Links.Statuses.Href == "" {
		return nil, nil
	}
	data, err := fetchAllPages[BuildStatus](ctx, c, pr.Links.Statuses.Href)
	if err != nil {
		return nil, err
	}
	for i := range data {
		data[i].PullRequestID = pr.ID
	}
	return data, nil
}

// fetchAllPullRequestList fetches all PR pages following "next" links.
func (c *Client) fetchAllPullRequestList(ctx context.Context, repo string) ([]PullRequestData, error) {
	return fetchAllPages[PullRequestData](ctx, c, c.buildFirstURL(repo))
}

// --- join helper ---

// mapPrWithOtherData joins PRs with their activity and diffstat records by PR ID.
func mapPrWithOtherData(
	prList []PullRequestData,
	activityList []PullRequestActivityData,
	diffStatList []DiffStatActivityData,
) map[int]*PullRequestReportData {
	res := make(map[int]*PullRequestReportData, len(prList))
	for _, pr := range prList {
		res[pr.ID] = &PullRequestReportData{
			pr:       pr,
			activity: []PullRequestActivityData{},
			diffstat: []DiffStatActivityData{},
		}
	}
	for _, act := range activityList {
		if r, ok := res[act.PullRequest.ID]; ok {
			r.activity = append(r.activity, act)
		}
	}
	for _, ds := range diffStatList {
		if r, ok := res[ds.PullRequestID]; ok {
			r.diffstat = append(r.diffstat, ds)
		}
	}
	return res
}

// --- flatten helpers (kept for test compatibility) ---

func flattenPRPages(pages []PullRequestList) []PullRequestData {
	var out []PullRequestData
	for _, p := range pages {
		out = append(out, p.Values...)
	}
	return out
}

func flattenActivityPages(pages []PullRequestActivity) []PullRequestActivityData {
	var out []PullRequestActivityData
	for _, p := range pages {
		out = append(out, p.Values...)
	}
	return out
}

func flattenDiffStatPages(pages []DiffStatActivity) []DiffStatActivityData {
	var out []DiffStatActivityData
	for _, p := range pages {
		for _, v := range p.Values {
			v.PullRequestID = p.PullRequestID
			out = append(out, v)
		}
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// --- data types ---

// PRParticipant is a reviewer or participant on a pull request.
type PRParticipant struct {
	DisplayName string `json:"display_name"`
	UUID        string `json:"uuid"`
	AccountID   string `json:"account_id"`
	Nickname    string `json:"nickname"`
	Type        string `json:"type"`
	Approved    bool   `json:"approved"`
	State       string `json:"state"`
	Role        string `json:"role"`
	Links       struct {
		Self   struct{ Href string `json:"href"` } `json:"self"`
		Avatar struct{ Href string `json:"href"` } `json:"avatar"`
		HTML   struct{ Href string `json:"href"` } `json:"html"`
	} `json:"links"`
}

// CommitData represents a single commit associated with a pull request.
type CommitData struct {
	Hash    string    `json:"hash"`
	Type    string    `json:"type"`
	Message string    `json:"message"`
	Date    time.Time `json:"date"`
	Author  struct {
		Type string `json:"type"`
		Raw  string `json:"raw"`
		User struct {
			DisplayName string `json:"display_name"`
			UUID        string `json:"uuid"`
			AccountID   string `json:"account_id"`
			Nickname    string `json:"nickname"`
			Type        string `json:"type"`
		} `json:"user"`
	} `json:"author"`
	Links struct {
		Self     struct{ Href string `json:"href"` } `json:"self"`
		HTML     struct{ Href string `json:"href"` } `json:"html"`
		Diff     struct{ Href string `json:"href"` } `json:"diff"`
		Statuses struct{ Href string `json:"href"` } `json:"statuses"`
	} `json:"links"`
	PullRequestID int // set during fetch
}

// CommentData represents a comment on a pull request.
type CommentData struct {
	ID        int       `json:"id"`
	CreatedOn time.Time `json:"created_on"`
	UpdatedOn time.Time `json:"updated_on"`
	Content   struct {
		Raw    string `json:"raw"`
		Markup string `json:"markup"`
		HTML   string `json:"html"`
	} `json:"content"`
	Inline struct {
		To   int    `json:"to"`
		From int    `json:"from"`
		Path string `json:"path"`
	} `json:"inline"`
	Parent struct {
		ID int `json:"id"`
	} `json:"parent"`
	User struct {
		DisplayName string `json:"display_name"`
		UUID        string `json:"uuid"`
		AccountID   string `json:"account_id"`
		Nickname    string `json:"nickname"`
		Type        string `json:"type"`
	} `json:"user"`
	Deleted       bool   `json:"deleted"`
	Type          string `json:"type"`
	PullRequestID int    // set during fetch
	Links         struct {
		Self struct{ Href string `json:"href"` } `json:"self"`
		HTML struct{ Href string `json:"href"` } `json:"html"`
	} `json:"links"`
}

// BuildStatus represents a build/CI status for a pull request.
type BuildStatus struct {
	State       string    `json:"state"`
	Key         string    `json:"key"`
	Name        string    `json:"name"`
	URL         string    `json:"url"`
	Description string    `json:"description"`
	CreatedOn   time.Time `json:"created_on"`
	UpdatedOn   time.Time `json:"updated_on"`
	Links       struct {
		Commit struct{ Href string `json:"href"` } `json:"commit"`
	} `json:"links"`
	PullRequestID int // set during fetch
}

// DiffStatActivityData holds per-file change statistics for a pull request.
type DiffStatActivityData struct {
	Type         string `json:"type"`
	LinesAdded   int    `json:"lines_added"`
	LinesRemoved int    `json:"lines_removed"`
	Status       string `json:"status"`
	Old          struct {
		Path        string `json:"path"`
		Type        string `json:"type"`
		EscapedPath string `json:"escaped_path"`
	} `json:"old"`
	New struct {
		Path        string `json:"path"`
		Type        string `json:"type"`
		EscapedPath string `json:"escaped_path"`
	} `json:"new"`
	PullRequestID int
}

// DiffStatActivity is the paginated API response for diffstats.
type DiffStatActivity struct {
	Values        []DiffStatActivityData `json:"values"`
	Pagelen       int                    `json:"pagelen"`
	Size          int                    `json:"size"`
	Page          int                    `json:"page"`
	PullRequestID int
}

// PullRequestActivityData is a single event in a PR's activity feed.
type PullRequestActivityData struct {
	PullRequest struct {
		Type  string `json:"type"`
		ID    int    `json:"id"`
		Title string `json:"title"`
		Links struct {
			Self struct{ Href string `json:"href"` } `json:"self"`
			HTML struct{ Href string `json:"href"` } `json:"html"`
		} `json:"links"`
	} `json:"pull_request"`
	Approval struct {
		Date time.Time `json:"date"`
		User struct {
			DisplayName string `json:"display_name"`
			Links       struct {
				Self   struct{ Href string `json:"href"` } `json:"self"`
				Avatar struct{ Href string `json:"href"` } `json:"avatar"`
				HTML   struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
			Type      string `json:"type"`
			UUID      string `json:"uuid"`
			AccountID string `json:"account_id"`
			Nickname  string `json:"nickname"`
		} `json:"user"`
		Pullrequest struct {
			Type  string `json:"type"`
			ID    int    `json:"id"`
			Title string `json:"title"`
			Links struct {
				Self struct{ Href string `json:"href"` } `json:"self"`
				HTML struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
		} `json:"pullrequest"`
	} `json:"approval,omitempty"`
	Comment struct {
		ID        int       `json:"id"`
		CreatedOn time.Time `json:"created_on"`
		UpdatedOn time.Time `json:"updated_on"`
		Content   struct {
			Type   string `json:"type"`
			Raw    string `json:"raw"`
			Markup string `json:"markup"`
			HTML   string `json:"html"`
		} `json:"content"`
		User struct {
			DisplayName string `json:"display_name"`
			Links       struct {
				Self   struct{ Href string `json:"href"` } `json:"self"`
				Avatar struct{ Href string `json:"href"` } `json:"avatar"`
				HTML   struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
			Type      string `json:"type"`
			UUID      string `json:"uuid"`
			AccountID string `json:"account_id"`
			Nickname  string `json:"nickname"`
		} `json:"user"`
		Deleted bool `json:"deleted"`
		Parent  struct {
			ID    int `json:"id"`
			Links struct {
				Self struct{ Href string `json:"href"` } `json:"self"`
				HTML struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
		} `json:"parent"`
		Type  string `json:"type"`
		Links struct {
			Self struct{ Href string `json:"href"` } `json:"self"`
			HTML struct{ Href string `json:"href"` } `json:"html"`
		} `json:"links"`
		Pullrequest struct {
			Type  string `json:"type"`
			ID    int    `json:"id"`
			Title string `json:"title"`
			Links struct {
				Self struct{ Href string `json:"href"` } `json:"self"`
				HTML struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
		} `json:"pullrequest"`
	} `json:"comment,omitempty"`
	Update struct {
		State       string `json:"state"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Reason      string `json:"reason"`
		Author      struct {
			DisplayName string `json:"display_name"`
			Links       struct {
				Self   struct{ Href string `json:"href"` } `json:"self"`
				Avatar struct{ Href string `json:"href"` } `json:"avatar"`
				HTML   struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
			Type      string `json:"type"`
			UUID      string `json:"uuid"`
			AccountID string `json:"account_id"`
			Nickname  string `json:"nickname"`
		} `json:"author"`
		Date time.Time `json:"date"`
	} `json:"update,omitempty"`
}

// PullRequestActivity is the paginated API response for activity events.
type PullRequestActivity struct {
	Values  []PullRequestActivityData `json:"values"`
	Pagelen int                       `json:"pagelen"`
	Next    string                    `json:"next"`
}

// PullRequestData holds the full metadata for a single pull request.
type PullRequestData struct {
	CommentCount      int         `json:"comment_count"`
	TaskCount         int         `json:"task_count"`
	Type              string      `json:"type"`
	ID                int         `json:"id"`
	Title             string      `json:"title"`
	Description       string      `json:"description"`
	State             string      `json:"state"`
	MergeCommit       interface{} `json:"merge_commit"`
	CloseSourceBranch bool        `json:"close_source_branch"`
	ClosedBy          interface{} `json:"closed_by"`
	Author            struct {
		DisplayName string `json:"display_name"`
		Links       struct {
			Self   struct{ Href string `json:"href"` } `json:"self"`
			Avatar struct{ Href string `json:"href"` } `json:"avatar"`
			HTML   struct{ Href string `json:"href"` } `json:"html"`
		} `json:"links"`
		Type      string `json:"type"`
		UUID      string `json:"uuid"`
		AccountID string `json:"account_id"`
		Nickname  string `json:"nickname"`
	} `json:"author"`
	Reviewers    []PRParticipant `json:"reviewers"`
	Participants []PRParticipant `json:"participants"`
	Reason       string          `json:"reason"`
	CreatedOn    time.Time       `json:"created_on"`
	UpdatedOn    time.Time       `json:"updated_on"`
	Destination  struct {
		Branch struct{ Name string `json:"name"` } `json:"branch"`
		Commit struct {
			Type  string `json:"type"`
			Hash  string `json:"hash"`
			Links struct {
				Self struct{ Href string `json:"href"` } `json:"self"`
				HTML struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
		} `json:"commit"`
		Repository struct {
			Type     string `json:"type"`
			FullName string `json:"full_name"`
			Links    struct {
				Self   struct{ Href string `json:"href"` } `json:"self"`
				HTML   struct{ Href string `json:"href"` } `json:"html"`
				Avatar struct{ Href string `json:"href"` } `json:"avatar"`
			} `json:"links"`
			Name string `json:"name"`
			UUID string `json:"uuid"`
		} `json:"repository"`
	} `json:"destination"`
	Source struct {
		Branch struct{ Name string `json:"name"` } `json:"branch"`
		Commit struct {
			Type  string `json:"type"`
			Hash  string `json:"hash"`
			Links struct {
				Self struct{ Href string `json:"href"` } `json:"self"`
				HTML struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
		} `json:"commit"`
		Repository struct {
			Type     string `json:"type"`
			FullName string `json:"full_name"`
			Links    struct {
				Self   struct{ Href string `json:"href"` } `json:"self"`
				HTML   struct{ Href string `json:"href"` } `json:"html"`
				Avatar struct{ Href string `json:"href"` } `json:"avatar"`
			} `json:"links"`
			Name string `json:"name"`
			UUID string `json:"uuid"`
		} `json:"repository"`
	} `json:"source"`
	Links struct {
		Self           struct{ Href string `json:"href"` } `json:"self"`
		HTML           struct{ Href string `json:"href"` } `json:"html"`
		Commits        struct{ Href string `json:"href"` } `json:"commits"`
		Approve        struct{ Href string `json:"href"` } `json:"approve"`
		RequestChanges struct{ Href string `json:"href"` } `json:"request-changes"`
		Diff           struct{ Href string `json:"href"` } `json:"diff"`
		Diffstat       struct{ Href string `json:"href"` } `json:"diffstat"`
		Comments       struct{ Href string `json:"href"` } `json:"comments"`
		Activity       struct{ Href string `json:"href"` } `json:"activity"`
		Merge          struct{ Href string `json:"href"` } `json:"merge"`
		Decline        struct{ Href string `json:"href"` } `json:"decline"`
		Statuses       struct{ Href string `json:"href"` } `json:"statuses"`
	} `json:"links"`
	Summary struct {
		Type   string `json:"type"`
		Raw    string `json:"raw"`
		Markup string `json:"markup"`
		HTML   string `json:"html"`
	} `json:"summary"`
}

// PullRequestList is the paginated API response for pull requests.
type PullRequestList struct {
	Values  []PullRequestData `json:"values"`
	Pagelen int               `json:"pagelen"`
	Size    int               `json:"size"`
	Page    int               `json:"page"`
	Next    string            `json:"next"`
}

// PullRequestReportData groups a PR with its associated activity and diffstat records.
type PullRequestReportData struct {
	pr       PullRequestData
	activity []PullRequestActivityData
	diffstat []DiffStatActivityData
}
