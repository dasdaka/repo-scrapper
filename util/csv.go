package util

import (
	"encoding/csv"
	"fmt"
	"os"
)

func exportActivityData(cfg Config, prData map[int]*PullRequestReportData, repoName string) (err error) {
	data := [][]string{
		{"ID", "SrcRepo", "SrcBranch", "DestRepo", "DestBranch", "Title", "Description", "State", "Author", "Created", "Updated",
			"FileChanged", "Added", "Removed", "Total",
			"Type", "User", "Content"},
	}

	for _, rep := range prData {
		fileChanged := 0
		added := 0
		removed := 0
		total := 0

		for _, diffStat := range rep.diffstat {
			fileChanged += 1
			added += diffStat.LinesAdded
			removed += diffStat.LinesRemoved
		}
		total = added + removed

		for _, act := range rep.activity {
			var cType string
			var user string
			var content string

			if len(act.Comment.User.DisplayName) > 0 {
				cType = act.Comment.Type
				user = act.Comment.User.DisplayName
				content = act.Comment.Content.Raw
			} else if len(act.Approval.User.DisplayName) > 0 {
				cType = "approval"
				user = act.Approval.User.DisplayName
				content = act.Approval.Date.Format("2006-01-02")
			} else {
				continue // exclude type "update"
			}

			data = append(data, []string{
				fmt.Sprintf("%d", rep.pr.ID),
				rep.pr.Source.Repository.FullName,
				rep.pr.Source.Branch.Name,
				rep.pr.Destination.Repository.FullName,
				rep.pr.Destination.Branch.Name,
				rep.pr.Title,
				rep.pr.Description,
				rep.pr.State,
				rep.pr.Author.DisplayName,
				rep.pr.CreatedOn.Format("2006-01-02"),
				rep.pr.UpdatedOn.Format("2006-01-02"),
				fmt.Sprintf("%d", fileChanged),
				fmt.Sprintf("%d", added),
				fmt.Sprintf("%d", removed),
				fmt.Sprintf("%d", total),
				cType,
				user,
				content,
			})
		}
	}

	file, err := os.Create(fmt.Sprintf(cfg.Report.ActivityFormatPath, repoName))
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, record := range data {
		err := writer.Write(record)
		if err != nil {
			return err
		}
	}
	return nil
}
