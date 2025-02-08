package main

import (
	"log"

	"github.com/david-kartopranoto/repo-scrapper/util"
)

func main() {
	config, err := util.LoadConfig("./config", "app.local")
	if err != nil {
		log.Fatal("Cannot load config:", err)
	}

	util.ScrapPullRequestToCSV(config)
}
