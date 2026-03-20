# repo-scrapper

Scrapes Bitbucket Cloud pull-request data into a PostgreSQL database and serves an interactive dashboard.

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Go | 1.20+ | `go version` |
| PostgreSQL | 17 recommended | Must be running locally |

### Install PostgreSQL (Windows — one-time)

```powershell
winget install PostgreSQL.PostgreSQL.17 --accept-package-agreements --accept-source-agreements `
  --override '--unattendedmodeui minimal --mode unattended --superpassword postgres --serverport 5432'
```

Binaries are installed to `C:\PostgreSQL17\bin\`. The Windows service `postgresql-x64-17` starts automatically.

### Create databases

```powershell
$env:PGPASSWORD = "postgres"
& "C:\PostgreSQL17\bin\psql.exe" -U postgres -c "CREATE DATABASE repo_scrapper;"
& "C:\PostgreSQL17\bin\psql.exe" -U postgres -c "CREATE DATABASE repo_scrapper_test;"
```

---

## Configuration

Copy the template and fill in your credentials:

```bash
cp config/app.yaml.template config/app.local.yaml
```

Edit `config/app.local.yaml`:

```yaml
bitbucket:
    token: Bearer <your-personal-access-token>
    workspace: <your-workspace-slug>
    repo_list:
        - repo-slug-1
        - repo-slug-2
    scrape_period: monthly   # daily | monthly | all
    query_filter: ""         # optional: e.g. 'state="MERGED"'
    pull_request_url: ""     # leave empty to use the default Bitbucket API URL

report:
    db_dsn: "postgres://postgres:postgres@localhost:5432/repo_scrapper?sslmode=disable"
    csv_export_path: ""      # optional: e.g. "reports/%s.csv"

dashboard:
    port: 8080
```

**`scrape_period` values:**
- `daily` — PRs updated since yesterday
- `monthly` — PRs updated since the 1st of the current month
- `all` — full history (no date restriction; can be slow on large repos)

---

## Build

```bash
go build -o app.exe ./cmd/app
```

---

## Commands

```
app <command>

  scrape      Fetch raw PR data from the Bitbucket API and store in the database
  aggregate   Build the pr_report summary table from stored raw data
  export      Export pr_report to CSV files (requires csv_export_path in config)
  all         Run scrape → aggregate → export in sequence  (default)
  serve       Start the web dashboard
```

### Run everything in one shot

```bash
./app.exe all
```

### Run steps individually

```bash
./app.exe scrape      # fetch from Bitbucket API
./app.exe aggregate   # rebuild pr_report from raw tables
./app.exe export      # write CSV files (skipped if csv_export_path is empty)
```

### Start the dashboard

```bash
./app.exe serve
```

Open **http://localhost:8080** in your browser.

---

## Dashboard

The web UI has two tabs:

### Dashboard tab
- Summary cards: total PRs, lines added/removed, unique authors and repos
- Charts: PR count by author, code changes, review activity, PRs by month, changes by repo
- Paginated PR table with filters (date range, repo, author)
- **Reload Data** button — refreshes the in-memory cache from the database without restarting

### Scraper tab
Run pipeline steps directly from the browser:
1. **Fetch Raw Data** — calls `scrape`
2. **Aggregate** — calls `aggregate` and reloads the dashboard
3. **Export CSV** — calls `export` (disabled if `csv_export_path` is not set)

Each operation streams live log output to the page.

---

## Running tests

Tests run against the `repo_scrapper_test` PostgreSQL database.

```bash
go test ./...
```

Override the test database connection string with the `TEST_DB_DSN` environment variable:

```bash
TEST_DB_DSN="postgres://user:pass@host:5432/mytest?sslmode=disable" go test ./...
```

---

## Project structure

```
cmd/app/          — main entry point, CLI commands
util/
  bitbucket.go    — Bitbucket API client, pagination, retry/rate-limit handling
  db.go           — PostgreSQL schema, upsert helpers
  aggregate.go    — reads raw tables, rebuilds pr_report
  export.go       — CSV export from pr_report
  config.go       — configuration types and loader
dashboard/
  server.go       — HTTP server, SSE scraper endpoints
  dashboard.go    — in-memory store, chart aggregation logic
  static/         — embedded index.html (single-page dark UI)
config/
  app.yaml.template   — configuration template
  app.local.yaml      — local overrides (git-ignored)
```
