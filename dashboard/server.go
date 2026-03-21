package dashboard

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/dasdaka/repo-scrapper/util"
)

//go:embed static/index.html
var indexHTML []byte

//go:embed static/pr_dashboard.html
var prDashboardHTML []byte

//go:embed static/pr_scraper.html
var prScraperHTML []byte

//go:embed static/pipelines.html
var pipelinesHTML []byte

// runner holds shared state for all HTTP handlers.
type runner struct {
	cfg           util.Config
	store         *DashboardStore
	pipelineStore *PipelineStore
}

// dualLog returns a util.LogFunc that writes to both the terminal logger and
// the SSE send callback so log lines appear in both places simultaneously.
func dualLog(send func(string)) util.LogFunc {
	return func(format string, args ...interface{}) {
		msg := fmt.Sprintf(format, args...)
		log.Print(msg)
		send(msg)
	}
}

// sseRun sets SSE response headers and calls fn. fn receives a send() callback
// that pushes a line to the client. A final "[DONE]" event is always emitted.
func sseRun(w http.ResponseWriter, fn func(send func(string))) {
	flusher, canFlush := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	send := func(line string) {
		fmt.Fprintf(w, "data: %s\n\n", line)
		if canFlush {
			flusher.Flush()
		}
	}
	fn(send)
	send("[DONE]")
}

// parseDateRange reads optional fromDate and toDate query parameters
// (format: YYYY-MM-DD) from the request and returns them as time.Time values.
// Zero values are returned for missing or unparseable parameters.
func parseDateRange(r *http.Request) (fromDate, toDate time.Time) {
	q := r.URL.Query()
	if v := q.Get("fromDate"); v != "" {
		fromDate, _ = time.Parse("2006-01-02", v)
		// fromDate is start-of-day (midnight) — inclusive lower bound, correct as-is.
	}
	if v := q.Get("toDate"); v != "" {
		if t, err := time.Parse("2006-01-02", v); err == nil {
			// Advance to end-of-day so that pipelines/PRs created anywhere within
			// the selected date are included in client-side comparisons.
			toDate = t.Add(24*time.Hour - time.Nanosecond)
		}
	}
	return
}

// --- PR scraper SSE handlers ---

func (rn *runner) runScrape(w http.ResponseWriter, r *http.Request) {
	fromDate, toDate := parseDateRange(r)
	sseRun(w, func(send func(string)) {
		logf := dualLog(send)
		db, err := util.OpenDB(rn.cfg.Report.DBConnStr)
		if err != nil {
			send("ERROR: " + err.Error())
			return
		}
		defer db.Close()
		if err := util.CreateSchema(r.Context(), db); err != nil {
			send("ERROR: " + err.Error())
			return
		}
		c := util.NewClientWithOptions(rn.cfg.Bitbucket, util.WithLogger(logf))
		if err := c.ScrapeRaw(r.Context(), db, fromDate, toDate); err != nil {
			send("ERROR: " + err.Error())
			return
		}
		if err := rn.store.Load(); err != nil {
			send("WARNING: store reload failed: " + err.Error())
		}
	})
}

func (rn *runner) runAggregate(w http.ResponseWriter, r *http.Request) {
	sseRun(w, func(send func(string)) {
		logf := dualLog(send)
		db, err := util.OpenDB(rn.cfg.Report.DBConnStr)
		if err != nil {
			send("ERROR: " + err.Error())
			return
		}
		defer db.Close()
		if err := util.CreateSchema(r.Context(), db); err != nil {
			send("ERROR: " + err.Error())
			return
		}
		if err := util.Aggregate(r.Context(), db, rn.cfg.Bitbucket.RepoList, logf); err != nil {
			send("ERROR: " + err.Error())
			return
		}
		if err := rn.store.Load(); err != nil {
			send("WARNING: store reload failed: " + err.Error())
		}
		// Aggregate() already calls AggregatePipelines internally; reload pipeline store too.
		if err := rn.pipelineStore.Load(); err != nil {
			send("WARNING: pipeline store reload failed: " + err.Error())
		}
	})
}

// --- Pipeline aggregate SSE handler ---

func (rn *runner) runAggregatePipelines(w http.ResponseWriter, r *http.Request) {
	sseRun(w, func(send func(string)) {
		logf := dualLog(send)
		db, err := util.OpenDB(rn.cfg.Report.DBConnStr)
		if err != nil {
			send("ERROR: " + err.Error())
			return
		}
		defer db.Close()
		if err := util.CreateSchema(r.Context(), db); err != nil {
			send("ERROR: " + err.Error())
			return
		}
		if err := util.AggregatePipelines(r.Context(), db, rn.cfg.Bitbucket.RepoList, logf); err != nil {
			send("ERROR: " + err.Error())
			return
		}
		if err := rn.pipelineStore.Load(); err != nil {
			send("WARNING: pipeline store reload failed: " + err.Error())
		}
	})
}

// Serve starts the dashboard HTTP server, loading data from the configured PostgreSQL DB.
func Serve(cfg util.Config) error {
	dsn := cfg.Report.DBConnStr
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/repo_scrapper?sslmode=disable"
	}

	// Ensure all tables exist before loading data. This is safe to call
	// repeatedly due to IF NOT EXISTS guards in the schema.
	if schemaDB, err := util.OpenDB(dsn); err == nil {
		if err := util.CreateSchema(context.Background(), schemaDB); err != nil {
			log.Printf("Warning: schema creation failed: %v", err)
		}
		schemaDB.Close()
	}

	store := NewStore(dsn, cfg.Dashboard.ExcludedAuthors)
	if err := store.Load(); err != nil {
		log.Printf("Warning: could not load PR data from DB %q: %v", dsn, err)
	} else {
		log.Printf("Loaded %d PR report rows from DB", store.Count())
	}

	pipelineStore := NewPipelineStore(dsn, cfg.Dashboard.ExcludedAuthors)
	if err := pipelineStore.Load(); err != nil {
		log.Printf("Warning: could not load pipeline data from DB: %v", err)
	} else {
		log.Printf("Loaded %d pipeline report rows from DB", pipelineStore.Count())
	}

	port := cfg.Dashboard.Port
	if port == 0 {
		port = 8080
	}

	rn := &runner{cfg: cfg, store: store, pipelineStore: pipelineStore}

	mux := http.NewServeMux()

	// --- PR dashboard routes ---

	mux.HandleFunc("/api/meta", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, BuildMeta(store.Activities(), store.BotSet(), store.ExcludedAuthors()))
	})
	mux.HandleFunc("/api/charts", func(w http.ResponseWriter, r *http.Request) {
		params := parseFilters(r)
		// Pre-filter by date, repo, and bots only; author/user filtering is
		// applied per-chart inside BuildCharts.
		dateRepoParams := FilterParams{DateFrom: params.DateFrom, DateTo: params.DateTo, Repos: params.Repos}
		rows := filterActivities(store.Activities(), dateRepoParams, store.BotSet())
		writeJSON(w, BuildCharts(rows, store.BotSet(), params))
	})
	mux.HandleFunc("/api/table", tableHandler(store))
	mux.HandleFunc("/api/reload", func(w http.ResponseWriter, r *http.Request) {
		if err := store.Load(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]int{"rows": store.Count()})
	})
	mux.HandleFunc("/api/run/scrape", rn.runScrape)
	mux.HandleFunc("/api/run/aggregate", rn.runAggregate)

	// --- Pipeline dashboard routes ---

	mux.HandleFunc("/api/pipeline/meta", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, BuildPipelineMeta(pipelineStore.Rows(), pipelineStore.BotSet(), pipelineStore.ExcludedAuthors(), rn.cfg.Bitbucket.ProductionRefs))
	})
	mux.HandleFunc("/api/pipeline/charts", func(w http.ResponseWriter, r *http.Request) {
		params := parsePipelineFilters(r)
		// Inject server-side production refs so BuildPipelineCharts can compute
		// the deployment frequency chart without an additional client round-trip.
		params.ProductionRefs = rn.cfg.Bitbucket.ProductionRefs
		// Pre-filter by date, repo, and bots only; creator/exclude filtering is
		// applied per-chart inside BuildPipelineCharts.
		preParams := PipelineFilterParams{DateFrom: params.DateFrom, DateTo: params.DateTo, Repos: params.Repos}
		rows := filterPipelineRows(pipelineStore.Rows(), preParams, pipelineStore.BotSet())
		writeJSON(w, BuildPipelineCharts(rows, pipelineStore.BotSet(), params))
	})
	mux.HandleFunc("/api/pipeline/table", pipelineTableHandler(pipelineStore))
	mux.HandleFunc("/api/pipeline/reload", func(w http.ResponseWriter, r *http.Request) {
		if err := pipelineStore.Load(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]int{"rows": pipelineStore.Count()})
	})
	mux.HandleFunc("/api/run/aggregate-pipelines", rn.runAggregatePipelines)

	// --- Static routes ---

	mux.HandleFunc("/pull-requests", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(prDashboardHTML)
	})
	mux.HandleFunc("/pull-requests/scraper", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(prScraperHTML)
	})
	mux.HandleFunc("/pipelines", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(pipelinesHTML)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Developer Productivity: http://localhost%s", addr)
	log.Printf("PR Dashboard:           http://localhost%s/pull-requests", addr)
	log.Printf("Pipeline Dashboard:     http://localhost%s/pipelines", addr)
	return http.ListenAndServe(addr, mux)
}

func tableHandler(store *DashboardStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		params := parseFilters(r)
		prs := deduplicatePRs(filterActivities(store.Activities(), params, store.BotSet()))

		page, pageSize := 1, 20
		fmt.Sscan(r.URL.Query().Get("page"), &page)
		fmt.Sscan(r.URL.Query().Get("pageSize"), &pageSize)
		if page < 1 {
			page = 1
		}
		if pageSize < 1 || pageSize > 200 {
			pageSize = 20
		}

		total := len(prs)
		start := (page - 1) * pageSize
		end := start + pageSize
		if start > total {
			start = total
		}
		if end > total {
			end = total
		}

		writeJSON(w, map[string]interface{}{
			"total":    total,
			"page":     page,
			"pageSize": pageSize,
			"data":     prs[start:end],
		})
	}
}

func pipelineTableHandler(store *PipelineStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		params := parsePipelineFilters(r)
		rows := filterPipelineRows(store.Rows(), params, store.BotSet())
		unique := deduplicatePipelineRows(rows)

		page, pageSize := 1, 20
		fmt.Sscan(r.URL.Query().Get("page"), &page)
		fmt.Sscan(r.URL.Query().Get("pageSize"), &pageSize)
		if page < 1 {
			page = 1
		}
		if pageSize < 1 || pageSize > 200 {
			pageSize = 20
		}

		total := len(unique)
		start := (page - 1) * pageSize
		end := start + pageSize
		if start > total {
			start = total
		}
		if end > total {
			end = total
		}

		writeJSON(w, map[string]interface{}{
			"total":    total,
			"page":     page,
			"pageSize": pageSize,
			"data":     unique[start:end],
		})
	}
}

func parseFilters(r *http.Request) FilterParams {
	q := r.URL.Query()
	var p FilterParams
	if v := q.Get("dateFrom"); v != "" {
		p.DateFrom, _ = time.Parse("2006-01-02", v)
	}
	if v := q.Get("dateTo"); v != "" {
		p.DateTo, _ = time.Parse("2006-01-02", v)
	}
	p.Repos        = filterEmpty(q["repos"])
	p.Authors      = filterEmpty(q["authors"])
	p.ExcludeUsers = filterEmpty(q["excludeUsers"])
	return p
}

func parsePipelineFilters(r *http.Request) PipelineFilterParams {
	q := r.URL.Query()
	var p PipelineFilterParams
	if v := q.Get("dateFrom"); v != "" {
		p.DateFrom, _ = time.Parse("2006-01-02", v)
	}
	if v := q.Get("dateTo"); v != "" {
		p.DateTo, _ = time.Parse("2006-01-02", v)
	}
	p.Repos        = filterEmpty(q["repos"])
	p.Creators     = filterEmpty(q["creators"])
	p.ExcludeUsers = filterEmpty(q["excludeUsers"])
	p.Environments = filterEmpty(q["environments"])
	p.Targets      = filterEmpty(q["targets"])
	p.ResultNames  = filterEmpty(q["resultNames"])
	return p
}

func filterEmpty(s []string) []string {
	var out []string
	for _, v := range s {
		if strings.TrimSpace(v) != "" {
			out = append(out, v)
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("JSON encode error: %v", err)
	}
}
