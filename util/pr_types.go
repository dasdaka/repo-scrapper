package util

import (
	"strings"
	"time"
)

// PRParticipant is a reviewer or participant on a pull request.
type PRParticipant struct {
	DisplayName string `json:"display_name"`
	UUID        string `json:"uuid"`
	AccountID   string `json:"account_id"`
	Nickname    string `json:"nickname"`
	Type        string `json:"type"`
	Approved    bool   `json:"approved"`
	State       string `json:"state"`
	Role        string `json:"role"`
	Links       struct {
		Self   struct{ Href string `json:"href"` } `json:"self"`
		Avatar struct{ Href string `json:"href"` } `json:"avatar"`
		HTML   struct{ Href string `json:"href"` } `json:"html"`
	} `json:"links"`
}

// CommitData represents a single commit associated with a pull request.
type CommitData struct {
	Hash    string    `json:"hash"`
	Type    string    `json:"type"`
	Message string    `json:"message"`
	Date    time.Time `json:"date"`
	Author  struct {
		Type string `json:"type"`
		Raw  string `json:"raw"`
		User struct {
			DisplayName string `json:"display_name"`
			UUID        string `json:"uuid"`
			AccountID   string `json:"account_id"`
			Nickname    string `json:"nickname"`
			Type        string `json:"type"`
		} `json:"user"`
	} `json:"author"`
	Links struct {
		Self     struct{ Href string `json:"href"` } `json:"self"`
		HTML     struct{ Href string `json:"href"` } `json:"html"`
		Diff     struct{ Href string `json:"href"` } `json:"diff"`
		Statuses struct{ Href string `json:"href"` } `json:"statuses"`
	} `json:"links"`
	PullRequestID int // set during fetch
}

// CommentData represents a comment on a pull request.
type CommentData struct {
	ID        int       `json:"id"`
	CreatedOn time.Time `json:"created_on"`
	UpdatedOn time.Time `json:"updated_on"`
	Content   struct {
		Raw    string `json:"raw"`
		Markup string `json:"markup"`
		HTML   string `json:"html"`
	} `json:"content"`
	Inline struct {
		To   int    `json:"to"`
		From int    `json:"from"`
		Path string `json:"path"`
	} `json:"inline"`
	Parent struct {
		ID int `json:"id"`
	} `json:"parent"`
	User struct {
		DisplayName string `json:"display_name"`
		UUID        string `json:"uuid"`
		AccountID   string `json:"account_id"`
		Nickname    string `json:"nickname"`
		Type        string `json:"type"`
	} `json:"user"`
	Deleted       bool   `json:"deleted"`
	Type          string `json:"type"`
	PullRequestID int    // set during fetch
	Links         struct {
		Self struct{ Href string `json:"href"` } `json:"self"`
		HTML struct{ Href string `json:"href"` } `json:"html"`
	} `json:"links"`
}

// BuildStatus represents a build/CI status for a pull request.
type BuildStatus struct {
	State       string    `json:"state"`
	Key         string    `json:"key"`
	Name        string    `json:"name"`
	URL         string    `json:"url"`
	Description string    `json:"description"`
	CreatedOn   time.Time `json:"created_on"`
	UpdatedOn   time.Time `json:"updated_on"`
	Links       struct {
		Commit struct{ Href string `json:"href"` } `json:"commit"`
	} `json:"links"`
	PullRequestID int // set during fetch
}

// DiffStatActivityData holds per-file change statistics for a pull request.
type DiffStatActivityData struct {
	Type         string `json:"type"`
	LinesAdded   int    `json:"lines_added"`
	LinesRemoved int    `json:"lines_removed"`
	Status       string `json:"status"`
	Old          struct {
		Path        string `json:"path"`
		Type        string `json:"type"`
		EscapedPath string `json:"escaped_path"`
	} `json:"old"`
	New struct {
		Path        string `json:"path"`
		Type        string `json:"type"`
		EscapedPath string `json:"escaped_path"`
	} `json:"new"`
	PullRequestID int
}

// DiffStatActivity is the paginated API response for diffstats.
type DiffStatActivity struct {
	Values        []DiffStatActivityData `json:"values"`
	Pagelen       int                    `json:"pagelen"`
	Size          int                    `json:"size"`
	Page          int                    `json:"page"`
	PullRequestID int
}

// PullRequestActivityData is a single event in a PR's activity feed.
type PullRequestActivityData struct {
	PullRequest struct {
		Type  string `json:"type"`
		ID    int    `json:"id"`
		Title string `json:"title"`
		Links struct {
			Self struct{ Href string `json:"href"` } `json:"self"`
			HTML struct{ Href string `json:"href"` } `json:"html"`
		} `json:"links"`
	} `json:"pull_request"`
	Approval struct {
		Date time.Time `json:"date"`
		User struct {
			DisplayName string `json:"display_name"`
			Links       struct {
				Self   struct{ Href string `json:"href"` } `json:"self"`
				Avatar struct{ Href string `json:"href"` } `json:"avatar"`
				HTML   struct{ Href string `json:"href"` } `json:"html"`
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
				Self struct{ Href string `json:"href"` } `json:"self"`
				HTML struct{ Href string `json:"href"` } `json:"html"`
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
				Self   struct{ Href string `json:"href"` } `json:"self"`
				Avatar struct{ Href string `json:"href"` } `json:"avatar"`
				HTML   struct{ Href string `json:"href"` } `json:"html"`
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
				Self struct{ Href string `json:"href"` } `json:"self"`
				HTML struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
		} `json:"parent"`
		Type  string `json:"type"`
		Links struct {
			Self struct{ Href string `json:"href"` } `json:"self"`
			HTML struct{ Href string `json:"href"` } `json:"html"`
		} `json:"links"`
		Pullrequest struct {
			Type  string `json:"type"`
			ID    int    `json:"id"`
			Title string `json:"title"`
			Links struct {
				Self struct{ Href string `json:"href"` } `json:"self"`
				HTML struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
		} `json:"pullrequest"`
	} `json:"comment,omitempty"`
	Update struct {
		State       string `json:"state"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Reason      string `json:"reason"`
		Author      struct {
			DisplayName string `json:"display_name"`
			Links       struct {
				Self   struct{ Href string `json:"href"` } `json:"self"`
				Avatar struct{ Href string `json:"href"` } `json:"avatar"`
				HTML   struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
			Type      string `json:"type"`
			UUID      string `json:"uuid"`
			AccountID string `json:"account_id"`
			Nickname  string `json:"nickname"`
		} `json:"author"`
		Date time.Time `json:"date"`
	} `json:"update,omitempty"`
}

// PullRequestActivity is the paginated API response for activity events.
type PullRequestActivity struct {
	Values  []PullRequestActivityData `json:"values"`
	Pagelen int                       `json:"pagelen"`
	Next    string                    `json:"next"`
}

// PullRequestData holds the full metadata for a single pull request.
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
			Self   struct{ Href string `json:"href"` } `json:"self"`
			Avatar struct{ Href string `json:"href"` } `json:"avatar"`
			HTML   struct{ Href string `json:"href"` } `json:"html"`
		} `json:"links"`
		Type      string `json:"type"`
		UUID      string `json:"uuid"`
		AccountID string `json:"account_id"`
		Nickname  string `json:"nickname"`
	} `json:"author"`
	Reviewers    []PRParticipant `json:"reviewers"`
	Participants []PRParticipant `json:"participants"`
	Reason       string          `json:"reason"`
	CreatedOn    time.Time       `json:"created_on"`
	UpdatedOn    time.Time       `json:"updated_on"`
	Destination  struct {
		Branch struct{ Name string `json:"name"` } `json:"branch"`
		Commit struct {
			Type  string `json:"type"`
			Hash  string `json:"hash"`
			Links struct {
				Self struct{ Href string `json:"href"` } `json:"self"`
				HTML struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
		} `json:"commit"`
		Repository struct {
			Type     string `json:"type"`
			FullName string `json:"full_name"`
			Links    struct {
				Self   struct{ Href string `json:"href"` } `json:"self"`
				HTML   struct{ Href string `json:"href"` } `json:"html"`
				Avatar struct{ Href string `json:"href"` } `json:"avatar"`
			} `json:"links"`
			Name string `json:"name"`
			UUID string `json:"uuid"`
		} `json:"repository"`
	} `json:"destination"`
	Source struct {
		Branch struct{ Name string `json:"name"` } `json:"branch"`
		Commit struct {
			Type  string `json:"type"`
			Hash  string `json:"hash"`
			Links struct {
				Self struct{ Href string `json:"href"` } `json:"self"`
				HTML struct{ Href string `json:"href"` } `json:"html"`
			} `json:"links"`
		} `json:"commit"`
		Repository struct {
			Type     string `json:"type"`
			FullName string `json:"full_name"`
			Links    struct {
				Self   struct{ Href string `json:"href"` } `json:"self"`
				HTML   struct{ Href string `json:"href"` } `json:"html"`
				Avatar struct{ Href string `json:"href"` } `json:"avatar"`
			} `json:"links"`
			Name string `json:"name"`
			UUID string `json:"uuid"`
		} `json:"repository"`
	} `json:"source"`
	Links struct {
		Self           struct{ Href string `json:"href"` } `json:"self"`
		HTML           struct{ Href string `json:"href"` } `json:"html"`
		Commits        struct{ Href string `json:"href"` } `json:"commits"`
		Approve        struct{ Href string `json:"href"` } `json:"approve"`
		RequestChanges struct{ Href string `json:"href"` } `json:"request-changes"`
		Diff           struct{ Href string `json:"href"` } `json:"diff"`
		Diffstat       struct{ Href string `json:"href"` } `json:"diffstat"`
		Comments       struct{ Href string `json:"href"` } `json:"comments"`
		Activity       struct{ Href string `json:"href"` } `json:"activity"`
		Merge          struct{ Href string `json:"href"` } `json:"merge"`
		Decline        struct{ Href string `json:"href"` } `json:"decline"`
		Statuses       struct{ Href string `json:"href"` } `json:"statuses"`
	} `json:"links"`
	Summary struct {
		Type   string `json:"type"`
		Raw    string `json:"raw"`
		Markup string `json:"markup"`
		HTML   string `json:"html"`
	} `json:"summary"`
}

// PullRequestList is the paginated API response for pull requests.
type PullRequestList struct {
	Values  []PullRequestData `json:"values"`
	Pagelen int               `json:"pagelen"`
	Size    int               `json:"size"`
	Page    int               `json:"page"`
	Next    string            `json:"next"`
}

// PRPipelineLink associates a pull request with a single pipeline UUID.
// One PR may have multiple links when multiple build statuses point to different pipelines.
type PRPipelineLink struct {
	PRID         int
	PipelineUUID string
}

// extractPipelineUUID parses the Bitbucket pipeline UUID from a build-status URL.
// Expected format: https://bitbucket.org/{workspace}/{repo}/pipelines/results/{uuid}
// Returns an empty string when the URL does not match the expected pattern.
func extractPipelineUUID(status BuildStatus) string {
	const marker = "/pipelines/results/"
	idx := strings.LastIndex(status.URL, marker)
	if idx == -1 {
		return ""
	}
	uuid := status.URL[idx+len(marker):]
	if i := strings.IndexAny(uuid, "?#"); i != -1 {
		uuid = uuid[:i]
	}
	return strings.TrimSpace(uuid)
}

// PullRequestReportData groups a PR with its associated activity, diffstat,
// and pipeline/deployment data resolved at aggregate time.
type PullRequestReportData struct {
	pr               PullRequestData
	activity         []PullRequestActivityData
	diffstat         []DiffStatActivityData
	pipelineUUID     string
	environmentName  string
	deploymentState  string
	deploymentStatus string
}
