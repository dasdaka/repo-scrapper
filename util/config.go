package util

import (
	"github.com/spf13/viper"
)

type DashboardConfig struct {
	Port int `mapstructure:"port"`
	// ExcludedAuthors is a list of display names to hide from all charts,
	// tables, and filter dropdowns (e.g. bots, service accounts).
	ExcludedAuthors []string `mapstructure:"excluded_authors"`
}

type Config struct {
	Bitbucket BitbucketConfig `mapstructure:"bitbucket"`
	Report    ReportConfig    `mapstructure:"report"`
	Dashboard DashboardConfig `mapstructure:"dashboard"`
}

type BitbucketConfig struct {
	Token     string   `mapstructure:"token"`
	Workspace string   `mapstructure:"workspace"`
	RepoList  []string `mapstructure:"repo_list"`
	// QueryFilter is an optional extra Bitbucket query clause appended after the
	// date range filter (e.g. `state="MERGED" OR state="DECLINED"`).
	// Date range is supplied at run-time via ScrapeRaw / ScrapePipelinesRaw;
	// use this field only for non-date predicates.
	QueryFilter string `mapstructure:"query_filter"`
	// PullRequestURL overrides the default Bitbucket Cloud API endpoint.
	// Must contain exactly three %s verbs: workspace, repo slug, query string.
	PullRequestURL string `mapstructure:"pull_request_url"`
	// ProductionEnvs is an optional list of deployment environment names to
	// filter pipeline and deployment scraping (e.g. ["Production", "Production-DR"]).
	// When non-empty, only deployments targeting a matching environment are stored,
	// and only pipelines linked to those deployments are stored.
	// Leave empty to scrape all environments.
	ProductionEnvs []string `mapstructure:"production_envs"`
	// ProductionRefs is an optional list of branch or tag names that represent
	// production deployments (e.g. ["master", "main", "release/*"]).
	// When set, the Deployment Frequency chart on the Pipelines dashboard counts
	// successful pipeline runs on these refs as production deployments.
	// Leave empty to hide the Deployment Frequency chart.
	ProductionRefs []string `mapstructure:"production_refs"`
}

type ReportConfig struct {
	// DBConnStr is a PostgreSQL connection string, e.g.
	// "postgres://user:password@localhost:5432/repo_scrapper?sslmode=disable"
	DBConnStr string `mapstructure:"db_dsn"`
}

// LoadConfig reads configuration from file or environment variables.
func LoadConfig(path string, name string) (config Config, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName(name)
	viper.SetConfigType("yaml")

	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}
