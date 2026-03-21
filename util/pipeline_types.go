package util

import (
	"encoding/json"
	"strings"
	"time"
)

// flexTime is a *time.Time that tolerates non-string JSON values (e.g. the
// boolean false that the Bitbucket API emits for first_successful and expired
// when the value is absent). Any non-string token is treated as a nil time.
type flexTime struct{ T *time.Time }

func (f *flexTime) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || data[0] != '"' {
		f.T = nil
		return nil
	}
	var t time.Time
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	f.T = &t
	return nil
}

func (f flexTime) MarshalJSON() ([]byte, error) {
	if f.T == nil {
		return []byte("null"), nil
	}
	return json.Marshal(f.T)
}

// Link is a generic Bitbucket HAL link.
type Link struct {
	Href string `json:"href"`
	Name string `json:"name,omitempty"`
}

// PipelineUser is the creator of a pipeline run as returned by the Bitbucket API.
type PipelineUser struct {
	DisplayName string `json:"display_name"`
	UUID        string `json:"uuid"`
	AccountID   string `json:"account_id"`
	Nickname    string `json:"nickname"`
	Type        string `json:"type"`
}

// PipelineTarget describes the branch or tag a pipeline runs against.
type PipelineTarget struct {
	// Type is the Bitbucket target type, e.g. "pipeline_branch_target".
	Type string `json:"type"`
	// RefType is "branch", "tag", or "bookmark".
	RefType string `json:"ref_type"`
	// RefName is the branch or tag name, e.g. "main" or "release/1.0".
	RefName string `json:"ref_name"`
}

// PipelineTrigger describes what initiated a pipeline run.
// Only the Type field is populated by the Bitbucket API.
type PipelineTrigger struct {
	// Type is the raw trigger type from the API, e.g. "pipeline_trigger_manual".
	Type string `json:"type"`
}

// PipelineStateResult holds the outcome of a completed pipeline.
type PipelineStateResult struct {
	// Type is the raw result type, e.g. "pipeline_state_completed_successful".
	Type string `json:"type"`
	// Name is the human-readable result: "SUCCESSFUL", "FAILED", or "ERROR".
	Name string `json:"name"`
}

// PipelineState holds the current state of a pipeline run.
type PipelineState struct {
	// Type is the raw state type, e.g. "pipeline_state_completed".
	Type string `json:"type"`
	// Name is the human-readable state: "COMPLETED", "IN_PROGRESS", "PENDING", or "STOPPED".
	Name   string              `json:"name"`
	Result PipelineStateResult `json:"result"`
}

// PipelineData represents a single Bitbucket pipeline run as returned by the
// /repositories/{workspace}/{repo}/pipelines API endpoint.
type PipelineData struct {
	UUID        string         `json:"uuid"`
	BuildNumber int            `json:"build_number"`
	RunNumber   int            `json:"run_number"`
	Creator     PipelineUser   `json:"creator"`
	Repository  struct {
		Name     string `json:"name"`
		FullName string `json:"full_name"`
	} `json:"repository"`
	Target           PipelineTarget  `json:"target"`
	Trigger          PipelineTrigger `json:"trigger"`
	State            PipelineState   `json:"state"`
	CreatedOn        time.Time  `json:"created_on"`
	CompletedOn      *time.Time `json:"completed_on"` // nil for in-progress pipelines
	BuildSecondsUsed int        `json:"build_seconds_used"`
	// FirstSuccessful and Expired are sent as boolean false by the Bitbucket API
	// when the value is absent, so flexTime silently treats non-string tokens as nil.
	FirstSuccessful flexTime `json:"first_successful"`
	Expired         flexTime `json:"expired"`
	HasVariables     bool            `json:"has_variables"`
	Links            struct {
		Self Link `json:"self"`
		HTML Link `json:"html"`
	} `json:"links"`
}

// TriggerName extracts a display-friendly trigger name from the raw Bitbucket
// trigger type string by stripping the "pipeline_trigger_" prefix and uppercasing.
// For example, "pipeline_trigger_manual" → "MANUAL".
func (p PipelineData) TriggerName() string {
	return strings.ToUpper(strings.TrimPrefix(p.Trigger.Type, "pipeline_trigger_"))
}

// --- Deployment data types ---

// DeploymentEnvironment is the named environment a pipeline step deploys to.
type DeploymentEnvironment struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
	Type string `json:"type"`
}


// DeploymentStatusResult holds the outcome of a completed deployment.
type DeploymentStatusResult struct {
	// Type is the raw status type, e.g. "deployment_state_completed_successful".
	Type string `json:"type"`
	// Name is the human-readable status: "SUCCESSFUL", "FAILED", or "ERROR".
	Name string `json:"name"`
}

// DeploymentState describes the current state of a deployment.
type DeploymentState struct {
	// Type is the raw state type, e.g. "deployment_state_completed".
	Type string `json:"type"`
	// Name is the human-readable state: "COMPLETED", "IN_PROGRESS", or "UNDEPLOYED".
	Name        string                 `json:"name"`
	Status      DeploymentStatusResult `json:"status"`
	StartedOn   *time.Time             `json:"started_on"`
	CompletedOn *time.Time             `json:"completed_on"`
}

// DeploymentCommitRef is a commit reference within a deployment release.
type DeploymentCommitRef struct {
	Hash string `json:"hash"`
	Type string `json:"type"`
}

// DeploymentData represents a single Bitbucket deployment as returned by the
// /repositories/{workspace}/{repo}/deployments API endpoint.
type DeploymentData struct {
	UUID    string          `json:"uuid"`
	State   DeploymentState `json:"state"`
	Pipeline struct {
		UUID string `json:"uuid"`
		Type string `json:"type"`
	} `json:"pipeline"`
	Step struct {
		UUID string `json:"uuid"`
	} `json:"step"`
	Environment DeploymentEnvironment `json:"environment"`
	Release     struct {
		Name   string              `json:"name"`
		UUID   string              `json:"uuid"`
		Tag    string              `json:"tag"`
		URL    string              `json:"url"`
		Commit DeploymentCommitRef `json:"commit"`
	} `json:"release"`
	CreatedOn      time.Time  `json:"created_on"`
	LastUpdateTime *time.Time `json:"last_update_time"`
	Links          struct {
		Self Link `json:"self"`
		HTML Link `json:"html"`
	} `json:"links"`
}

// PipelineStep represents a single step within a Bitbucket pipeline run as
// returned by /pipelines/{uuid}/steps. When a step deploys to an environment,
// DeploymentUUID is set and can be used with /deployments/{uuid} to fetch details.
// RawJSON is preserved so we can inspect unknown fields if DeploymentUUID is empty.
type PipelineStep struct {
	UUID           string          `json:"uuid"`
	Name           string          `json:"name"`
	Type           string          `json:"type"`
	DeploymentUUID string          `json:"deployment_uuid"`
	RawJSON        json.RawMessage `json:"-"` // populated manually after unmarshal
}

// PipelineReportData groups a PipelineData with its associated deployments.
type PipelineReportData struct {
	pipeline    PipelineData
	deployments []DeploymentData
}
