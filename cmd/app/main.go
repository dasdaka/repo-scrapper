package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/dasdaka/repo-scrapper/dashboard"
	"github.com/dasdaka/repo-scrapper/util"
)

const usage = `Usage: app <command>

Commands:
  scrape      Fetch raw data from the Bitbucket API and store in the database
  aggregate   Build the pr_report summary table from the stored raw data
  all         Run scrape → aggregate in sequence  (default)
  serve       Start the web dashboard
`

func main() {
	cmd := "all"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	switch cmd {
	case "scrape":
		mustRun(runScrape)
	case "aggregate":
		mustRun(runAggregate)
	case "all":
		mustRun(runScrape)
		mustRun(runAggregate)
	case "serve":
		runServe()
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}
}

// mustRun calls fn and exits on error.
func mustRun(fn func() error) {
	if err := fn(); err != nil {
		log.Fatal(err)
	}
}

// openDB loads config, opens the database, and ensures the schema exists.
// The caller is responsible for closing the returned *sql.DB.
func openDB() (util.Config, *sql.DB, error) {
	cfg, err := util.LoadConfig("./config", "app.local")
	if err != nil {
		return cfg, nil, fmt.Errorf("load config: %w", err)
	}
	db, err := util.OpenDB(cfg.Report.DBConnStr)
	if err != nil {
		return cfg, nil, fmt.Errorf("open db: %w", err)
	}
	if err := util.CreateSchema(context.Background(), db); err != nil {
		db.Close()
		return cfg, nil, fmt.Errorf("create schema: %w", err)
	}
	return cfg, db, nil
}

// runScrape fetches raw data from the Bitbucket API and stores it in the DB.
func runScrape() error {
	cfg, db, err := openDB()
	if err != nil {
		return err
	}
	defer db.Close()

	log.Println("scrape: fetching raw data from Bitbucket API")
	c := util.NewClient(cfg.Bitbucket)
	if err := c.ScrapeRaw(context.Background(), db); err != nil {
		return fmt.Errorf("scrape: %w", err)
	}
	log.Println("scrape: done")
	return nil
}

// runAggregate rebuilds pr_report from the raw tables already in the DB.
func runAggregate() error {
	cfg, db, err := openDB()
	if err != nil {
		return err
	}
	defer db.Close()

	log.Println("aggregate: building pr_report")
	if err := util.Aggregate(context.Background(), db, cfg.Bitbucket.RepoList, util.TerminalLog); err != nil {
		return fmt.Errorf("aggregate: %w", err)
	}
	log.Println("aggregate: done")
	return nil
}

// runServe starts the web dashboard.
func runServe() {
	cfg, err := util.LoadConfig("./config", "app.local")
	if err != nil {
		log.Fatal("load config:", err)
	}
	if err := dashboard.Serve(cfg); err != nil {
		log.Fatal("serve:", err)
	}
}
