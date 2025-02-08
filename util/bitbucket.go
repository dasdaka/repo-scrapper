package util

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

var client = &http.Client{
	Timeout: 10 * time.Second,
}

type DiffStatActivityData struct {
	Type         string `json:"type"`
	LinesAdded   int    `json:"lines_added"`
	LinesRemoved int    `json:"lines_removed"`
	Status       string `json:"status"`
	Old          struct {
		Path        string `json:"path"`
		Type        string `json:"type"`
		EscapedPath string `json:"escaped_path"`
		Links       struct {
			Self struct {
				Href string `json:"href"`
			} `json:"self"`
		} `json:"links"`
	} `json:"old"`
	New struct {
		Path        string `json:"path"`
		Type        string `json:"type"`
		EscapedPath string `json:"escaped_path"`
		Links       struct {
			Self struct {
				Href string `json:"href"`
			} `json:"self"`
		} `json:"links"`
	} `json:"new"`
	PullRequestID int
}

type DiffStatActivity struct {
	Values        []DiffStatActivityData `json:"values"`
	Pagelen       int                    `json:"pagelen"`
	Size          int                    `json:"size"`
	Page          int                    `json:"page"`
	PullRequestID int
}

type PullRequestActivityData struct {
	PullRequest struct {
		Type  string `json:"type"`
		ID    int    `json:"id"`
		Title string `json:"title"`
		Links struct {
			Self struct {
				Href string `json:"href"`
			} `json:"self"`
			HTML struct {
				Href string `json:"href"`
			} `json:"html"`
		} `json:"links"`
	} `json:"pull_request"`
	Approval struct {
		Date time.Time `json:"date"`
		User struct {
			DisplayName string `json:"display_name"`
			Links       struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
				Avatar struct {
					Href string `json:"href"`
				} `json:"avatar"`
				HTML struct {
					Href string `json:"href"`
				} `json:"html"`
			} `json:"links"`
			Type      string `json:"type"`
			UUID      string `json:"uuid"`
			AccountID string `json:"account_id"`
			Nickname  string `json:"nickname"`
		} `json:"user"`
		Pullrequest struct {
			Type  string `json:"type"`
			ID    int    `json:"id"`
			Title string `json:"title"`
			Links struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
				HTML struct {
					Href string `json:"href"`
				} `json:"html"`
			} `json:"links"`
		} `json:"pullrequest"`
	} `json:"approval,omitempty"`
	Comment struct {
		ID        int       `json:"id"`
		CreatedOn time.Time `json:"created_on"`
		UpdatedOn time.Time `json:"updated_on"`
		Content   struct {
			Type   string `json:"type"`
			Raw    string `json:"raw"`
			Markup string `json:"markup"`
			HTML   string `json:"html"`
		} `json:"content"`
		User struct {
			DisplayName string `json:"display_name"`
			Links       struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
				Avatar struct {
					Href string `json:"href"`
				} `json:"avatar"`
				HTML struct {
					Href string `json:"href"`
				} `json:"html"`
			} `json:"links"`
			Type      string `json:"type"`
			UUID      string `json:"uuid"`
			AccountID string `json:"account_id"`
			Nickname  string `json:"nickname"`
		} `json:"user"`
		Deleted bool `json:"deleted"`
		Parent  struct {
			ID    int `json:"id"`
			Links struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
				HTML struct {
					Href string `json:"href"`
				} `json:"html"`
			} `json:"links"`
		} `json:"parent"`
		Type  string `json:"type"`
		Links struct {
			Self struct {
				Href string `json:"href"`
			} `json:"self"`
			HTML struct {
				Href string `json:"href"`
			} `json:"html"`
		} `json:"links"`
		Pullrequest struct {
			Type  string `json:"type"`
			ID    int    `json:"id"`
			Title string `json:"title"`
			Links struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
				HTML struct {
					Href string `json:"href"`
				} `json:"html"`
			} `json:"links"`
		} `json:"pullrequest"`
	} `json:"comment,omitempty"`
	Update struct {
		State       string `json:"state"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Reviewers   []struct {
			DisplayName string `json:"display_name"`
			Links       struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
				Avatar struct {
					Href string `json:"href"`
				} `json:"avatar"`
				HTML struct {
					Href string `json:"href"`
				} `json:"html"`
			} `json:"links"`
			Type      string `json:"type"`
			UUID      string `json:"uuid"`
			AccountID string `json:"account_id"`
			Nickname  string `json:"nickname"`
		} `json:"reviewers"`
		Changes struct {
			Status struct {
				Old string `json:"old"`
				New string `json:"new"`
			} `json:"status"`
		} `json:"changes"`
		Reason string `json:"reason"`
		Author struct {
			DisplayName string `json:"display_name"`
			Links       struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
				Avatar struct {
					Href string `json:"href"`
				} `json:"avatar"`
				HTML struct {
					Href string `json:"href"`
				} `json:"html"`
			} `json:"links"`
			Type      string `json:"type"`
			UUID      string `json:"uuid"`
			AccountID string `json:"account_id"`
			Nickname  string `json:"nickname"`
		} `json:"author"`
		Date        time.Time `json:"date"`
		Destination struct {
			Branch struct {
				Name string `json:"name"`
			} `json:"branch"`
			Commit struct {
				Type  string `json:"type"`
				Hash  string `json:"hash"`
				Links struct {
					Self struct {
						Href string `json:"href"`
					} `json:"self"`
					HTML struct {
						Href string `json:"href"`
					} `json:"html"`
				} `json:"links"`
			} `json:"commit"`
			Repository struct {
				Type     string `json:"type"`
				FullName string `json:"full_name"`
				Links    struct {
					Self struct {
						Href string `json:"href"`
					} `json:"self"`
					HTML struct {
						Href string `json:"href"`
					} `json:"html"`
					Avatar struct {
						Href string `json:"href"`
					} `json:"avatar"`
				} `json:"links"`
				Name string `json:"name"`
				UUID string `json:"uuid"`
			} `json:"repository"`
		} `json:"destination"`
		Source struct {
			Branch struct {
				Name string `json:"name"`
			} `json:"branch"`
			Commit struct {
				Type  string `json:"type"`
				Hash  string `json:"hash"`
				Links struct {
					Self struct {
						Href string `json:"href"`
					} `json:"self"`
					HTML struct {
						Href string `json:"href"`
					} `json:"html"`
				} `json:"links"`
			} `json:"commit"`
			Repository struct {
				Type     string `json:"type"`
				FullName string `json:"full_name"`
				Links    struct {
					Self struct {
						Href string `json:"href"`
					} `json:"self"`
					HTML struct {
						Href string `json:"href"`
					} `json:"html"`
					Avatar struct {
						Href string `json:"href"`
					} `json:"avatar"`
				} `json:"links"`
				Name string `json:"name"`
				UUID string `json:"uuid"`
			} `json:"repository"`
		} `json:"source"`
	} `json:"update,omitempty"`
}

type PullRequestActivity struct {
	Values  []PullRequestActivityData `json:"values"`
	Pagelen int                       `json:"pagelen"`
	Next    string                    `json:"next"`
}

type PullRequestData struct {
	CommentCount      int         `json:"comment_count"`
	TaskCount         int         `json:"task_count"`
	Type              string      `json:"type"`
	ID                int         `json:"id"`
	Title             string      `json:"title"`
	Description       string      `json:"description"`
	State             string      `json:"state"`
	MergeCommit       interface{} `json:"merge_commit"`
	CloseSourceBranch bool        `json:"close_source_branch"`
	ClosedBy          interface{} `json:"closed_by"`
	Author            struct {
		DisplayName string `json:"display_name"`
		Links       struct {
			Self struct {
				Href string `json:"href"`
			} `json:"self"`
			Avatar struct {
				Href string `json:"href"`
			} `json:"avatar"`
			HTML struct {
				Href string `json:"href"`
			} `json:"html"`
		} `json:"links"`
		Type      string `json:"type"`
		UUID      string `json:"uuid"`
		AccountID string `json:"account_id"`
		Nickname  string `json:"nickname"`
	} `json:"author"`
	Reason      string    `json:"reason"`
	CreatedOn   time.Time `json:"created_on"`
	UpdatedOn   time.Time `json:"updated_on"`
	Destination struct {
		Branch struct {
			Name string `json:"name"`
		} `json:"branch"`
		Commit struct {
			Type  string `json:"type"`
			Hash  string `json:"hash"`
			Links struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
				HTML struct {
					Href string `json:"href"`
				} `json:"html"`
			} `json:"links"`
		} `json:"commit"`
		Repository struct {
			Type     string `json:"type"`
			FullName string `json:"full_name"`
			Links    struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
				HTML struct {
					Href string `json:"href"`
				} `json:"html"`
				Avatar struct {
					Href string `json:"href"`
				} `json:"avatar"`
			} `json:"links"`
			Name string `json:"name"`
			UUID string `json:"uuid"`
		} `json:"repository"`
	} `json:"destination"`
	Source struct {
		Branch struct {
			Name string `json:"name"`
		} `json:"branch"`
		Commit struct {
			Type  string `json:"type"`
			Hash  string `json:"hash"`
			Links struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
				HTML struct {
					Href string `json:"href"`
				} `json:"html"`
			} `json:"links"`
		} `json:"commit"`
		Repository struct {
			Type     string `json:"type"`
			FullName string `json:"full_name"`
			Links    struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
				HTML struct {
					Href string `json:"href"`
				} `json:"html"`
				Avatar struct {
					Href string `json:"href"`
				} `json:"avatar"`
			} `json:"links"`
			Name string `json:"name"`
			UUID string `json:"uuid"`
		} `json:"repository"`
	} `json:"source"`
	Links struct {
		Self struct {
			Href string `json:"href"`
		} `json:"self"`
		HTML struct {
			Href string `json:"href"`
		} `json:"html"`
		Commits struct {
			Href string `json:"href"`
		} `json:"commits"`
		Approve struct {
			Href string `json:"href"`
		} `json:"approve"`
		RequestChanges struct {
			Href string `json:"href"`
		} `json:"request-changes"`
		Diff struct {
			Href string `json:"href"`
		} `json:"diff"`
		Diffstat struct {
			Href string `json:"href"`
		} `json:"diffstat"`
		Comments struct {
			Href string `json:"href"`
		} `json:"comments"`
		Activity struct {
			Href string `json:"href"`
		} `json:"activity"`
		Merge struct {
			Href string `json:"href"`
		} `json:"merge"`
		Decline struct {
			Href string `json:"href"`
		} `json:"decline"`
		Statuses struct {
			Href string `json:"href"`
		} `json:"statuses"`
	} `json:"links"`
	Summary struct {
		Type   string `json:"type"`
		Raw    string `json:"raw"`
		Markup string `json:"markup"`
		HTML   string `json:"html"`
	} `json:"summary"`
}

type PullRequestList struct {
	Values  []PullRequestData `json:"values"`
	Pagelen int               `json:"pagelen"`
	Size    int               `json:"size"`
	Page    int               `json:"page"`
	Next    string            `json:"next"`
}

type PullRequestReportData struct {
	pr       PullRequestData
	activity []PullRequestActivityData
	diffstat []DiffStatActivityData
}

func ScrapPullRequestToCSV(cfg Config) {
	for _, repo := range cfg.Bitbucket.RepoList {

		res, err := fetchAllPullRequestList(cfg, repo)
		if err != nil {
			log.Fatal(err)
		}

		var prDataList []PullRequestData
		for _, pr := range res {
			for _, value := range pr.Values {
				prDataList = append(prDataList, value)
			}
		}

		res1, err1 := fetchAllPullRequestActivity(cfg, prDataList)
		if err1 != nil {
			log.Fatal(err1)
		}

		var activityList []PullRequestActivityData
		for _, activity := range res1 {
			for _, value := range activity.Values {
				activityList = append(activityList, value)
			}
		}

		res2, err2 := fetchAllPullRequestDiffStat(cfg, prDataList)
		if err1 != nil {
			log.Fatal(err1)
		}

		var diffStatList []DiffStatActivityData
		for _, diffstat := range res2 {
			for _, value := range diffstat.Values {
				value.PullRequestID = diffstat.PullRequestID
				diffStatList = append(diffStatList, value)
			}
		}

		reportData, err2 := mapPrWithOtherData(prDataList, activityList, diffStatList)
		if err2 != nil {
			log.Fatal(err2)
		}

		exportActivityData(cfg, reportData, repo)

	}
}

func mapPrWithOtherData(prList []PullRequestData, activityList []PullRequestActivityData, diffStatList []DiffStatActivityData) (res map[int]*PullRequestReportData, err error) {
	res = make(map[int]*PullRequestReportData, len(prList))
	for _, pr := range prList {
		res[pr.ID] = &PullRequestReportData{
			pr:       pr,
			activity: []PullRequestActivityData{},
			diffstat: []DiffStatActivityData{},
		}
	}

	for _, activity := range activityList {
		res[activity.PullRequest.ID].activity = append(res[activity.PullRequest.ID].activity, activity)
	}

	for _, diffstat := range diffStatList {
		res[diffstat.PullRequestID].diffstat = append(res[diffstat.PullRequestID].diffstat, diffstat)
	}

	return res, nil
}

func fetchPullRequestDiffStat(cfg Config, prData PullRequestData) (res DiffStatActivity, err error) {
	url := fmt.Sprintf("%s", prData.Links.Diffstat.Href)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", cfg.Bitbucket.Token)
	resp, err := client.Do(req)
	if err != nil {
		return res, err
	}

	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	err = json.Unmarshal(body, &res)
	if err != nil {
		return res, err
	}

	res.PullRequestID = prData.ID

	return res, err
}

func fetchAllPullRequestDiffStat(cfg Config, prList []PullRequestData) (res []DiffStatActivity, err error) {
	var wg sync.WaitGroup
	pages := len(prList)
	results := make(chan DiffStatActivity, pages)

	for _, pr := range prList {
		wg.Add(1)
		go func(cfg Config, data PullRequestData, wg *sync.WaitGroup, results chan<- DiffStatActivity) {
			defer wg.Done()
			res, err := fetchPullRequestDiffStat(cfg, data)
			if err == nil {
				results <- res
			}
		}(cfg, pr, &wg, results)
	}

	wg.Wait()
	close(results)

	for pr := range results {
		res = append(res, pr)
	}

	return res, err
}

func fetchPullRequestActivity(cfg Config, prData PullRequestData) (res PullRequestActivity, err error) {
	url := fmt.Sprintf("%s?pagelen=%d", prData.Links.Activity.Href, cfg.Bitbucket.ActivityPagelen)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", cfg.Bitbucket.Token)
	resp, err := client.Do(req)
	if err != nil {
		return res, err
	}

	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	err = json.Unmarshal(body, &res)
	if err != nil {
		return res, err
	}

	return res, err
}

func fetchAllPullRequestActivity(cfg Config, prList []PullRequestData) (res []PullRequestActivity, err error) {
	var wg sync.WaitGroup
	pages := len(prList)
	results := make(chan PullRequestActivity, pages)

	for _, pr := range prList {
		wg.Add(1)
		go func(cfg Config, data PullRequestData, wg *sync.WaitGroup, results chan<- PullRequestActivity) {
			defer wg.Done()
			res, err := fetchPullRequestActivity(cfg, data)
			if err == nil {
				results <- res
			}
		}(cfg, pr, &wg, results)
	}

	wg.Wait()
	close(results)

	for pr := range results {
		res = append(res, pr)
	}

	return res, err
}

func fetchPullRequestList(cfg Config, repo string, page int) (res PullRequestList, err error) {
	url := fmt.Sprintf(cfg.Bitbucket.PullRequestURL, cfg.Bitbucket.Workspace, repo, page, cfg.Bitbucket.PRPagelen, cfg.Bitbucket.QueryFilter)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", cfg.Bitbucket.Token)
	resp, err := client.Do(req)
	if err != nil {
		return res, err
	}

	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	err = json.Unmarshal(body, &res)
	if err != nil {
		return res, err
	}

	return res, err
}

func fetchAllPullRequestList(cfg Config, repo string) (res []PullRequestList, err error) {
	prList, err := fetchPullRequestList(cfg, repo, 1)
	if err != nil {
		return
	}
	res = append(res, prList)

	if cfg.Bitbucket.PRPagelen <= 1 {
		return
	}

	var wg sync.WaitGroup
	pages := min(prList.Size, cfg.Bitbucket.MaxPage)
	if pages == 1 {
		return
	}

	results := make(chan PullRequestList, pages)

	for i := 2; i <= pages; i++ {
		wg.Add(1)
		go func(cfg Config, page int, wg *sync.WaitGroup, results chan<- PullRequestList) {
			defer wg.Done()
			res, err := fetchPullRequestList(cfg, repo, page)
			if err == nil {
				results <- res
			}
		}(cfg, i, &wg, results)
	}

	wg.Wait()
	close(results)

	for pr := range results {
		res = append(res, pr)
	}

	return res, err
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}
