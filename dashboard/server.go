package dashboard

import (
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

// runner holds shared state for the scraper API handlers.
type runner struct {
	cfg   util.Config
	store *DashboardStore
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

func (rn *runner) runScrape(w http.ResponseWriter, r *http.Request) {
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
		if err := c.ScrapeRaw(r.Context(), db); err != nil {
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
	})
}

// Serve starts the dashboard HTTP server, loading data from the configured PostgreSQL DB.
func Serve(cfg util.Config) error {
	dsn := cfg.Report.DBConnStr
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/repo_scrapper?sslmode=disable"
	}

	store := NewStore(dsn)
	if err := store.Load(); err != nil {
		log.Printf("Warning: could not load data from DB %q: %v", dsn, err)
	} else {
		log.Printf("Loaded %d report rows from DB", store.Count())
	}

	port := cfg.Dashboard.Port
	if port == 0 {
		port = 8080
	}

	rn := &runner{cfg: cfg, store: store}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/meta", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, BuildMeta(store.Activities()))
	})
	mux.HandleFunc("/api/charts", func(w http.ResponseWriter, r *http.Request) {
		params := parseFilters(r)
		writeJSON(w, BuildCharts(filterActivities(store.Activities(), params)))
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
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Dashboard running at http://localhost%s", addr)
	return http.ListenAndServe(addr, mux)
}

func tableHandler(store *DashboardStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		params := parseFilters(r)
		prs := deduplicatePRs(filterActivities(store.Activities(), params))

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

func parseFilters(r *http.Request) FilterParams {
	q := r.URL.Query()
	var p FilterParams
	if v := q.Get("dateFrom"); v != "" {
		p.DateFrom, _ = time.Parse("2006-01-02", v)
	}
	if v := q.Get("dateTo"); v != "" {
		p.DateTo, _ = time.Parse("2006-01-02", v)
	}
	p.Repos = filterEmpty(q["repos"])
	p.Authors = filterEmpty(q["authors"])
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
