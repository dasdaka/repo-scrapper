package util

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
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

// --- http execution ---

// do executes a request, retrying on transport errors and HTTP 429.
// It respects Retry-After and X-RateLimit-NearLimit headers.
// All rate-limit and retry events are logged with a [rate-limit] or [retry] prefix
// so both the PR and pipeline scrapers emit consistent, identifiable messages.
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
			delay := jitter(backoff)
			c.logf("[retry] transport error on attempt %d/%d: %v — retrying in %s",
				attempt, c.retry.MaxAttempts, err, delay.Round(time.Millisecond))
			if sleepErr := ctxSleep(ctx, delay); sleepErr != nil {
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
			c.logf("[rate-limit] HTTP 429: sleeping %s before retry (attempt %d/%d)",
				wait.Round(time.Millisecond), attempt, c.retry.MaxAttempts)
			if sleepErr := ctxSleep(ctx, wait); sleepErr != nil {
				return nil, sleepErr
			}
			backoff = minDuration(backoff*2, c.retry.MaxBackoff)
			continue
		}

		// Near the rate limit — add a small proactive delay before returning.
		if resp.Header.Get("X-RateLimit-NearLimit") == "1" {
			throttleDelay := jitter(c.retry.BaseBackoff)
			c.logf("[rate-limit] X-RateLimit-NearLimit: proactive throttle %s",
				throttleDelay.Round(time.Millisecond))
			if sleepErr := ctxSleep(ctx, throttleDelay); sleepErr != nil {
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
// When label is non-empty each fetched page is logged with its record count for
// progress visibility; pass an empty string for high-frequency sub-resource fetches
// (commits, activity, comments, statuses) to keep the output concise.
func fetchAllPages[T any](ctx context.Context, c *Client, firstURL, label string) ([]T, error) {
	var all []T
	pageURL := firstURL
	pageNum := 1
	for pageURL != "" {
		var p pageResponse[T]
		if err := c.getJSON(ctx, pageURL, &p); err != nil {
			return nil, err
		}
		all = append(all, p.Values...)
		if label != "" {
			c.logf("%s: page %d — %d records (%d total)", label, pageNum, len(p.Values), len(all))
		}
		pageNum++
		pageURL = p.Next
	}
	return all, nil
}

// fetchPagesUntil fetches pages starting from firstURL, calling stop(item) on each
// item in every page. When stop returns true for any item, pagination halts after
// that page is fully consumed. Pass nil to fetch all pages (equivalent to fetchAllPages).
// This is used for APIs sorted descending by date — once an item older than fromDate
// is seen, all subsequent pages will be older still, so we can exit early.
func fetchPagesUntil[T any](ctx context.Context, c *Client, firstURL, label string, stop func(T) bool) ([]T, error) {
	var all []T
	pageURL := firstURL
	pageNum := 1
	for pageURL != "" {
		var p pageResponse[T]
		if err := c.getJSON(ctx, pageURL, &p); err != nil {
			return nil, err
		}
		all = append(all, p.Values...)
		if label != "" {
			c.logf("%s: page %d — %d records (%d total)", label, pageNum, len(p.Values), len(all))
		}
		pageNum++
		if stop != nil {
			for _, item := range p.Values {
				if stop(item) {
					return all, nil
				}
			}
		}
		pageURL = p.Next
	}
	return all, nil
}
