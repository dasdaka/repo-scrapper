package util

import (
	"github.com/spf13/viper"
)

type Config struct {
	Bitbucket BitbucketConfig `mapstructure:"bitbucket"`
	Report    ReportConfig    `mapstructure:"report"`
}

type BitbucketConfig struct {
	Token           string   `mapstructure:"token"`
	Workspace       string   `mapstructure:"workspace"`
	RepoList        []string `mapstructure:"repo_list"`
	PRPagelen       int      `mapstructure:"pr_page_len"`
	ActivityPagelen int      `mapstructure:"activity_page_len"`
	MaxPage         int      `mapstructure:"max_page"`
	QueryFilter     string   `mapstructure:"query_filter"`
	PullRequestURL  string   `mapstructure:"pull_request_url"`
}

type ReportConfig struct {
	ActivityFormatPath string `mapstructure:"activity_format_path"`
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
