package util

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// --- query & URL helpers ---

// buildQueryFilter builds a Bitbucket API q= filter string from an explicit date
// range and an optional extra clause (QueryFilter config, e.g. `state="MERGED"`).
//
// fromDate and toDate produce created_on predicates when non-zero; both are
// optional and independent. extraFilter is appended after the date clauses.
// All parts are joined with " AND ".
func buildQueryFilter(fromDate, toDate time.Time, extraFilter string) string {
	var parts []string
	if !fromDate.IsZero() {
		parts = append(parts, fmt.Sprintf(`created_on >= "%s"`, fromDate.Format("2006-01-02")))
	}
	if !toDate.IsZero() {
		parts = append(parts, fmt.Sprintf(`created_on <= "%s"`, toDate.Format("2006-01-02")))
	}
	if extraFilter != "" {
		parts = append(parts, extraFilter)
	}
	return strings.Join(parts, " AND ")
}

// formatDateRange returns a human-readable "from → to" string for log output.
// Zero values are shown as "*" (no bound).
func formatDateRange(fromDate, toDate time.Time) string {
	from, to := "*", "*"
	if !fromDate.IsZero() {
		from = fromDate.Format("2006-01-02")
	}
	if !toDate.IsZero() {
		to = toDate.Format("2006-01-02")
	}
	return from + " → " + to
}

// buildPRFirstURL returns the first-page URL for the PR list of the given repo.
// fromDate and toDate restrict results by created_on; pass zero values for no
// date filter. When PullRequestURL is configured it is used as a template
// (three %s: workspace, repo, query); otherwise the default Bitbucket Cloud
// endpoint is used.
func (c *Client) buildPRFirstURL(repo string, fromDate, toDate time.Time) string {
	qFilter := buildQueryFilter(fromDate, toDate, c.cfg.QueryFilter)
	if c.cfg.PullRequestURL != "" {
		return fmt.Sprintf(c.cfg.PullRequestURL, c.cfg.Workspace, repo, url.QueryEscape(qFilter))
	}
	// Include all PR states so the date filter is not limited to only OPEN PRs.
	base := fmt.Sprintf(
		"https://api.bitbucket.org/2.0/repositories/%s/%s/pullrequests?pagelen=%d&sort=-updated_on&state=OPEN&state=MERGED&state=DECLINED&state=SUPERSEDED",
		url.PathEscape(c.cfg.Workspace), url.PathEscape(repo), defaultPagelen,
	)
	if qFilter != "" {
		base += "&q=" + url.QueryEscape(qFilter)
	}
	return base
}

// buildFirstURL is a backward-compatible shim calling buildPRFirstURL with no
// date filter. Tests in bitbucket_test.go call this method.
func (c *Client) buildFirstURL(repo string) string {
	return c.buildPRFirstURL(repo, time.Time{}, time.Time{})
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
//
// fromDate and toDate restrict the created_on range of pull requests fetched
// from the API. Pass zero values to fetch all available pull requests.
func (c *Client) ScrapeRaw(ctx context.Context, db *sql.DB, fromDate, toDate time.Time) error {
	n := len(c.cfg.RepoList)
	c.logf("[scrape-pr] starting: %d repo(s) to process (range: %s)", n, formatDateRange(fromDate, toDate))
	totalStart := time.Now()
	for i, repo := range c.cfg.RepoList {
		c.logf("[scrape-pr] [%d/%d] %s: begin", i+1, n, repo)
		repoStart := time.Now()
		if err := c.scrapeRawData(ctx, db, repo, fromDate, toDate); err != nil {
			return fmt.Errorf("repo %s: %w", repo, err)
		}
		c.logf("[scrape-pr] [%d/%d] %s: completed in %s",
			i+1, n, repo, time.Since(repoStart).Round(time.Millisecond))
	}
	c.logf("[scrape-pr] all %d repo(s) done in %s", n, time.Since(totalStart).Round(time.Second))
	return nil
}

// --- internal scraping ---

func (c *Client) scrapeRawData(ctx context.Context, db *sql.DB, repo string, fromDate, toDate time.Time) error {
	// --- Pull request list ---
	c.logf("[%s] fetching pull requests (range: %s)", repo, formatDateRange(fromDate, toDate))
	firstURL := c.buildPRFirstURL(repo, fromDate, toDate)
	c.logf("[%s] PR list URL: %s", repo, firstURL)
	t0 := time.Now()
	prs, err := fetchAllPages[PullRequestData](ctx, c, firstURL, "["+repo+"] PRs")
	if err != nil {
		return fmt.Errorf("fetch PR list: %w", err)
	}
	c.logf("[%s] %d pull requests fetched in %s", repo, len(prs), time.Since(t0).Round(time.Millisecond))

	t0 = time.Now()
	if err := UpsertPullRequests(ctx, db, repo, prs); err != nil {
		return fmt.Errorf("store pull_requests: %w", err)
	}
	c.logf("[%s] stored %d pull requests (%s)", repo, len(prs), time.Since(t0).Round(time.Millisecond))

	if len(prs) == 0 {
		return nil
	}

	// --- Per-PR sub-data (concurrent) ---
	concurrency := cap(c.sem)
	c.logf("[%s] fetching sub-data for %d PRs (concurrency=%d)", repo, len(prs), concurrency)
	t0 = time.Now()

	type result struct {
		data prSubData
		err  error
	}
	results := make([]result, len(prs))
	var wg sync.WaitGroup
	var fetchedCount atomic.Int32
	total := int32(len(prs))
	for i, pr := range prs {
		wg.Add(1)
		go func(idx int, pr PullRequestData) {
			defer wg.Done()
			c.sem.acquire()
			defer c.sem.release()
			data, err := c.fetchAllPRData(ctx, pr)
			results[idx] = result{data, err}
			done := fetchedCount.Add(1)
			if done%10 == 0 || done == total {
				c.logf("[%s] sub-data progress: %d/%d PRs processed", repo, done, total)
			}
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
	c.logf("[%s] sub-data fetched: %d commits, %d activity, %d diffstat, %d comments, %d statuses (%s)",
		repo, len(allCommits), len(allActivity), len(allDiffstat), len(allComments), len(allStatuses),
		time.Since(t0).Round(time.Millisecond))

	// --- Store sub-data ---
	t0 = time.Now()
	if err := UpsertPRCommits(ctx, db, repo, allCommits); err != nil {
		return fmt.Errorf("store pr_commits: %w", err)
	}
	c.logf("[%s] stored %d commits (%s)", repo, len(allCommits), time.Since(t0).Round(time.Millisecond))

	t0 = time.Now()
	if err := UpsertPRActivity(ctx, db, repo, allActivity); err != nil {
		return fmt.Errorf("store pr_activity: %w", err)
	}
	c.logf("[%s] stored %d activity events (%s)", repo, len(allActivity), time.Since(t0).Round(time.Millisecond))

	t0 = time.Now()
	if err := UpsertPRDiffStat(ctx, db, repo, allDiffstat); err != nil {
		return fmt.Errorf("store pr_diffstat: %w", err)
	}
	c.logf("[%s] stored %d diffstats (%s)", repo, len(allDiffstat), time.Since(t0).Round(time.Millisecond))

	t0 = time.Now()
	if err := UpsertPRComments(ctx, db, repo, allComments); err != nil {
		return fmt.Errorf("store pr_comments: %w", err)
	}
	c.logf("[%s] stored %d comments (%s)", repo, len(allComments), time.Since(t0).Round(time.Millisecond))

	t0 = time.Now()
	if err := UpsertPRStatuses(ctx, db, repo, allStatuses); err != nil {
		return fmt.Errorf("store pr_statuses: %w", err)
	}
	c.logf("[%s] stored %d statuses (%s)", repo, len(allStatuses), time.Since(t0).Round(time.Millisecond))

	// --- Pipeline links (driven by build-status URLs) ---
	// Each PR's build statuses carry URLs pointing to Bitbucket pipeline results.
	// We extract the pipeline UUID from those URLs and store them in pr_pipelines
	// so the aggregate join can link each PR to its CI/build pipeline.
	// Note: these are build pipelines (feature-branch CI), not deployment pipelines.
	// Deployment frequency is tracked separately via production-branch pipeline runs.
	t0 = time.Now()
	var allPipelineLinks []PRPipelineLink
	var newPipelines []PipelineData
	seenUUIDs := make(map[string]bool)

	for _, s := range allStatuses {
		uuid := extractPipelineUUID(s)
		if uuid == "" {
			continue
		}
		allPipelineLinks = append(allPipelineLinks, PRPipelineLink{PRID: s.PullRequestID, PipelineUUID: uuid})

		if seenUUIDs[uuid] {
			continue
		}
		seenUUIDs[uuid] = true

		cached, err := PipelineExistsByUUID(ctx, db, repo, uuid)
		if err != nil {
			c.logf("[%s] WARN: pipeline cache check for %s: %v", repo, uuid, err)
		}
		if cached {
			continue
		}

		p, err := c.fetchPipelineByUUID(ctx, repo, uuid)
		if err != nil {
			c.logf("[%s] WARN: fetch pipeline %s: %v", repo, uuid, err)
			continue
		}
		newPipelines = append(newPipelines, p)
	}

	if len(newPipelines) > 0 {
		if err := UpsertPipelines(ctx, db, repo, newPipelines); err != nil {
			return fmt.Errorf("store pipelines: %w", err)
		}
	}
	if err := UpsertPRPipelines(ctx, db, repo, allPipelineLinks); err != nil {
		return fmt.Errorf("store pr_pipelines: %w", err)
	}
	c.logf("[%s] pipelines: %d new, %d links (%s)",
		repo, len(newPipelines), len(allPipelineLinks), time.Since(t0).Round(time.Millisecond))

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
	return fetchAllPages[CommitData](ctx, c, pr.Links.Commits.Href, "")
}

func (c *Client) fetchAllActivity(ctx context.Context, pr PullRequestData) ([]PullRequestActivityData, error) {
	if pr.Links.Activity.Href == "" {
		return nil, nil
	}
	firstURL := fmt.Sprintf("%s?pagelen=%d", pr.Links.Activity.Href, defaultPagelen)
	data, err := fetchAllPages[PullRequestActivityData](ctx, c, firstURL, "")
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
	data, err := fetchAllPages[CommentData](ctx, c, pr.Links.Comments.Href, "")
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
	data, err := fetchAllPages[BuildStatus](ctx, c, pr.Links.Statuses.Href, "")
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
	return fetchAllPages[PullRequestData](ctx, c, c.buildFirstURL(repo), "")
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
