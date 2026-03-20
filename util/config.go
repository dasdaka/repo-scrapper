package util

import (
	"github.com/spf13/viper"
)

type DashboardConfig struct {
	Port int `mapstructure:"port"`
}

type Config struct {
	Bitbucket BitbucketConfig `mapstructure:"bitbucket"`
	Report    ReportConfig    `mapstructure:"report"`
	Dashboard DashboardConfig `mapstructure:"dashboard"`
}

type BitbucketConfig struct {
	Token      string   `mapstructure:"token"`
	Workspace  string   `mapstructure:"workspace"`
	RepoList   []string `mapstructure:"repo_list"`
	// ScrapePeriod controls the lookback window: "daily", "monthly", or "all".
	// When set to "daily" or "monthly", an updated_on filter is added automatically.
	ScrapePeriod   string `mapstructure:"scrape_period"`
	// QueryFilter is an optional extra Bitbucket query clause appended to the
	// generated date filter (e.g. `state="MERGED" OR state="DECLINED"`).
	QueryFilter    string `mapstructure:"query_filter"`
	// PullRequestURL overrides the default Bitbucket Cloud API endpoint.
	// Must contain exactly three %s verbs: workspace, repo slug, query string.
	PullRequestURL string `mapstructure:"pull_request_url"`
}

type ReportConfig struct {
	// CSVExportPath is a fmt format string with one %s for the repo name
	// (e.g. "activity-%s.csv"). Leave empty to skip CSV export.
	CSVExportPath string `mapstructure:"csv_export_path"`
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
