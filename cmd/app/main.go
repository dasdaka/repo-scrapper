package main

import (
	"log"

	"github.com/dasdaka/repo-scrapper/util"
)

func main() {
	config, err := util.LoadConfig("./config", "app.local")
	if err != nil {
		log.Fatal("Cannot load config:", err)
	}

	util.ScrapPullRequestToCSV(config)
}
