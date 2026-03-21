package dashboard

import (
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// PipelineRow mirrors one row in the pipeline_report table. It represents a
// single (pipeline, deployment environment) pair; pipelines with no deployment
// records have empty environment fields.
type PipelineRow struct {
	PipelineUUID     string    `json:"pipelineUuid"`
	Repo             string    `json:"repo"`
	BuildNumber      int       `json:"buildNumber"`
	RunNumber        int       `json:"runNumber"`
	Creator          string    `json:"creator"`
	TargetRefType    string    `json:"targetRefType"`
	TargetRefName    string    `json:"targetRefName"`
	TriggerName      string    `json:"triggerName"`
	StateName        string    `json:"stateName"`
	ResultName       string    `json:"resultName"`
	EnvironmentUUID  string    `json:"environmentUuid"`
	EnvironmentName  string    `json:"environmentName"`
	DeploymentState  string    `json:"deploymentState"`
	DeploymentStatus string    `json:"deploymentStatus"`
	CreatedOn        time.Time `json:"createdOn"`
	CompletedOn      time.Time `json:"completedOn"`
	DurationSeconds  int       `json:"durationSeconds"`
}

// PipelineFilterParams carries parsed query parameters for the pipeline dashboard.
type PipelineFilterParams struct {
	DateFrom       time.Time
	DateTo         time.Time
	Repos          []string
	Creators       []string // include filter: matched against Creator field
	ExcludeUsers   []string // exclude filter: removes rows where Creator matches
	Environments   []string // environment name include filter
	Targets        []string // branch/tag ref_name include filter
	ResultNames    []string // result name include filter: SUCCESSFUL, FAILED, ERROR
	ProductionRefs []string // branch/tag names treated as production (from server config)
}

// PipelineStore holds in-memory pipeline_report data loaded from the DB.
type PipelineStore struct {
	mu              sync.RWMutex
	rows            []PipelineRow
	dsn             string
	botSet          map[string]bool
	excludedAuthors []string
}

// NewPipelineStore creates a PipelineStore backed by the given DSN. Entries in
// excludedAuthors are used to pre-populate the bot set and the Exclude filter.
func NewPipelineStore(dsn string, excludedAuthors []string) *PipelineStore {
	return &PipelineStore{
		dsn:             dsn,
		botSet:          toSet(excludedAuthors),
		excludedAuthors: excludedAuthors,
	}
}

// BotSet returns the set of excluded creator names derived from config.
func (s *PipelineStore) BotSet() map[string]bool { return s.botSet }

// ExcludedAuthors returns the configured excluded creator names.
func (s *PipelineStore) ExcludedAuthors() []string { return s.excludedAuthors }

// Load queries the pipeline_report table and refreshes the in-memory cache.
func (s *PipelineStore) Load() error {
	db, err := sql.Open("postgres", s.dsn)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT
			pipeline_uuid, repo, build_number, run_number,
			creator, target_ref_type, target_ref_name, trigger_name,
			state_name, result_name,
			environment_uuid, environment_name, deployment_state, deployment_status,
			created_on, completed_on, duration_seconds
		FROM pipeline_report
		ORDER BY created_on DESC
	`)
	if err != nil {
		return fmt.Errorf("query pipeline_report: %w", err)
	}
	defer rows.Close()

	var result []PipelineRow
	for rows.Next() {
		var r PipelineRow
		var createdOn, completedOn string
		if err := rows.Scan(
			&r.PipelineUUID, &r.Repo, &r.BuildNumber, &r.RunNumber,
			&r.Creator, &r.TargetRefType, &r.TargetRefName, &r.TriggerName,
			&r.StateName, &r.ResultName,
			&r.EnvironmentUUID, &r.EnvironmentName, &r.DeploymentState, &r.DeploymentStatus,
			&createdOn, &completedOn, &r.DurationSeconds,
		); err != nil {
			return err
		}
		r.CreatedOn, _ = time.Parse(time.RFC3339, createdOn)
		r.CompletedOn, _ = time.Parse(time.RFC3339, completedOn)
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	s.rows = result
	s.mu.Unlock()
	return nil
}

// Rows returns the in-memory pipeline rows (safe for concurrent reads).
func (s *PipelineStore) Rows() []PipelineRow {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rows
}

// Count returns the number of pipeline rows in the cache.
func (s *PipelineStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rows)
}

// filterPipelineRows returns rows that satisfy all constraints in p and are not
// in the bot set. Filtering is applied on the Creator field for creator-related
// filters; environments and targets filter their respective fields.
func filterPipelineRows(rows []PipelineRow, p PipelineFilterParams, bots map[string]bool) []PipelineRow {
	creatorSet := toSet(p.Creators)
	excludeSet := toSet(p.ExcludeUsers)
	envSet     := toSet(p.Environments)
	targetSet  := toSet(p.Targets)
	resultSet  := toSet(p.ResultNames)
	repoSet    := toSet(p.Repos)

	var out []PipelineRow
	for _, r := range rows {
		if bots[r.Creator] {
			continue
		}
		if len(excludeSet) > 0 && excludeSet[r.Creator] {
			continue
		}
		if !p.DateFrom.IsZero() && r.CreatedOn.Before(p.DateFrom) {
			continue
		}
		if !p.DateTo.IsZero() && r.CreatedOn.After(p.DateTo) {
			continue
		}
		if len(repoSet) > 0 && !repoSet[r.Repo] {
			continue
		}
		if len(creatorSet) > 0 && !creatorSet[r.Creator] {
			continue
		}
		if len(envSet) > 0 && !envSet[r.EnvironmentName] {
			continue
		}
		if len(targetSet) > 0 && !targetSet[r.TargetRefName] {
			continue
		}
		if len(resultSet) > 0 && !resultSet[r.ResultName] {
			continue
		}
		out = append(out, r)
	}
	return out
}

// deduplicatePipelineRows returns one PipelineRow per unique pipeline UUID.
// When a pipeline has multiple deployment rows, the first occurrence is kept.
// Use this for per-pipeline charts; deployment-frequency charts use all rows.
func deduplicatePipelineRows(rows []PipelineRow) []PipelineRow {
	seen := make(map[string]bool)
	var out []PipelineRow
	for _, r := range rows {
		if seen[r.PipelineUUID] {
			continue
		}
		seen[r.PipelineUUID] = true
		out = append(out, r)
	}
	return out
}

// --- Aggregation types ---

// PipelineSummary holds aggregate counts for the pipeline dashboard summary cards.
type PipelineSummary struct {
	TotalPipelines     int `json:"totalPipelines"`
	SuccessCount       int `json:"successCount"`
	FailedCount        int `json:"failedCount"`
	ErrorCount         int `json:"errorCount"`
	StoppedCount       int `json:"stoppedCount"`
	AvgDurationSeconds int `json:"avgDurationSeconds"`
	UniqueCreators     int `json:"uniqueCreators"`
	UniqueRepos        int `json:"uniqueRepos"`
}

// PipelineResultBreakdown shows per-result counts for a single repository.
// Used in the "Pipeline Results by Repository" stacked bar chart to highlight
// repos that have the most non-successful pipelines.
type PipelineResultBreakdown struct {
	Label      string `json:"label"`
	Successful int    `json:"successful"`
	Failed     int    `json:"failed"`
	Error      int    `json:"error"`
	Stopped    int    `json:"stopped"`
}

// DeploymentFrequencyPoint is a SUCCESSFUL deployment count for a
// (environment, month) combination. Used in the DORA Deployment Frequency panel.
type DeploymentFrequencyPoint struct {
	Month       string `json:"month"`
	Environment string `json:"environment"`
	Count       int    `json:"count"`
}

// PipelineChartsResponse holds all chart data for the pipeline dashboard.
type PipelineChartsResponse struct {
	Summary             PipelineSummary            `json:"summary"`
	DeploymentFrequency []DeploymentFrequencyPoint `json:"deploymentFrequency"`
	ResultsByRepo       []PipelineResultBreakdown  `json:"resultsByRepo"`
	PipelinesByCreator  []LabelValue               `json:"pipelinesByCreator"`
	AvgDurationByRepo   []LabelValue               `json:"avgDurationByRepo"`
	PipelinesByMonth    []LabelValue               `json:"pipelinesByMonth"`
}

// PipelineMetaResponse is returned by /api/pipeline/meta for populating the
// filter dropdowns in the pipeline dashboard.
type PipelineMetaResponse struct {
	Repos           []string `json:"repos"`
	Creators        []string `json:"creators"`
	Environments    []string `json:"environments"`
	Targets         []string `json:"targets"`
	ExcludedAuthors []string `json:"excludedAuthors"`
	ProductionRefs  []string `json:"productionRefs"`
	DateMin         string   `json:"dateMin"`
	DateMax         string   `json:"dateMax"`
}

// BuildPipelineMeta scans all rows to find distinct filter options and date bounds.
// bots is the set of excluded creator names (from config); pass nil to include everyone.
// productionRefs is the server-side config list of production branch/tag names.
func BuildPipelineMeta(rows []PipelineRow, bots map[string]bool, excludedAuthors []string, productionRefs []string) PipelineMetaResponse {
	repoSet    := make(map[string]bool)
	creatorSet := make(map[string]bool)
	envSet     := make(map[string]bool)
	targetSet  := make(map[string]bool)
	var minDate, maxDate time.Time

	for _, r := range rows {
		repoSet[r.Repo] = true
		if r.Creator != "" && !bots[r.Creator] {
			creatorSet[r.Creator] = true
		}
		if r.EnvironmentName != "" {
			envSet[r.EnvironmentName] = true
		}
		if r.TargetRefName != "" {
			targetSet[r.TargetRefName] = true
		}
		if !r.CreatedOn.IsZero() {
			if minDate.IsZero() || r.CreatedOn.Before(minDate) {
				minDate = r.CreatedOn
			}
			if r.CreatedOn.After(maxDate) {
				maxDate = r.CreatedOn
			}
		}
	}

	dateMin, dateMax := "", ""
	if !minDate.IsZero() {
		dateMin = minDate.Format("2006-01-02")
	}
	if !maxDate.IsZero() {
		dateMax = maxDate.Format("2006-01-02")
	}

	return PipelineMetaResponse{
		Repos:           sortedKeys(repoSet),
		Creators:        sortedKeys(creatorSet),
		Environments:    sortedKeys(envSet),
		Targets:         sortedKeys(targetSet),
		ExcludedAuthors: excludedAuthors,
		ProductionRefs:  productionRefs,
		DateMin:         dateMin,
		DateMax:         dateMax,
	}
}

// BuildPipelineCharts aggregates filtered pipeline rows into chart data.
// bots is the set of bot creator names; pass nil to include everyone.
//
// Most charts deduplicate by pipeline UUID (one entry per pipeline run).
// The Deployment Frequency chart uses all rows to count deployments per
// (environment, month), since one pipeline can deploy to multiple environments.
func BuildPipelineCharts(rows []PipelineRow, bots map[string]bool, p PipelineFilterParams) PipelineChartsResponse {
	// Apply bot, creator-include, and creator-exclude filters per-chart,
	// matching only the Creator field (mirrors how BuildCharts handles Author).
	creatorSet := toSet(p.Creators)
	excludeSet := toSet(p.ExcludeUsers)

	var filteredRows []PipelineRow
	for _, r := range rows {
		if bots[r.Creator] {
			continue
		}
		if len(creatorSet) > 0 && !creatorSet[r.Creator] {
			continue
		}
		if excludeSet[r.Creator] {
			continue
		}
		filteredRows = append(filteredRows, r)
	}

	// Deduplicate by pipeline UUID for all per-pipeline charts.
	unique := deduplicatePipelineRows(filteredRows)

	// Summary aggregation over deduplicated pipelines.
	creatorCountSet := make(map[string]bool)
	repoCountSet    := make(map[string]bool)
	totalDuration, completedCount := 0, 0
	successCount, failedCount, errorCount, stoppedCount := 0, 0, 0, 0

	for _, r := range unique {
		if r.Creator != "" {
			creatorCountSet[r.Creator] = true
		}
		repoCountSet[r.Repo] = true
		if r.DurationSeconds > 0 {
			totalDuration += r.DurationSeconds
			completedCount++
		}
		switch r.ResultName {
		case "SUCCESSFUL":
			successCount++
		case "FAILED":
			failedCount++
		case "ERROR":
			errorCount++
		case "STOPPED":
			stoppedCount++
		}
	}

	avgDuration := 0
	if completedCount > 0 {
		avgDuration = totalDuration / completedCount
	}

	// Pipelines by creator (deduplicated).
	byCreator := make(map[string]int)
	for _, r := range unique {
		if r.Creator != "" {
			byCreator[r.Creator]++
		}
	}

	// Pipelines by month using CreatedOn (deduplicated).
	byMonth := make(map[string]int)
	for _, r := range unique {
		if !r.CreatedOn.IsZero() {
			byMonth[r.CreatedOn.Format("2006-01")]++
		}
	}

	// Results by repo (deduplicated): basis for the stacked "health" bar chart.
	byRepo := make(map[string]*PipelineResultBreakdown)
	for _, r := range unique {
		if _, ok := byRepo[r.Repo]; !ok {
			byRepo[r.Repo] = &PipelineResultBreakdown{Label: r.Repo}
		}
		switch r.ResultName {
		case "SUCCESSFUL":
			byRepo[r.Repo].Successful++
		case "FAILED":
			byRepo[r.Repo].Failed++
		case "ERROR":
			byRepo[r.Repo].Error++
		case "STOPPED":
			byRepo[r.Repo].Stopped++
		}
	}

	// Average duration by repo (deduplicated).
	dursByRepo := make(map[string][]int)
	for _, r := range unique {
		if r.DurationSeconds > 0 {
			dursByRepo[r.Repo] = append(dursByRepo[r.Repo], r.DurationSeconds)
		}
	}
	avgDurByRepo := make([]LabelValue, 0, len(dursByRepo))
	for repo, durs := range dursByRepo {
		total := 0
		for _, d := range durs {
			total += d
		}
		avgDurByRepo = append(avgDurByRepo, LabelValue{Label: repo, Value: total / len(durs)})
	}

	// Deployment frequency: successful pipeline runs on configured production branches,
	// grouped by (branch, month). Uses deduplicated rows (one per pipeline run).
	// Production branches are set via the server-side ProductionRefs config field
	// (e.g. ["master", "main"]) — when empty the chart is hidden.
	type envMonthKey struct{ env, month string }
	freqMap := make(map[envMonthKey]int)
	prodRefSet := toSet(p.ProductionRefs)
	if len(prodRefSet) > 0 {
		for _, r := range unique {
			if r.ResultName == "SUCCESSFUL" && prodRefSet[r.TargetRefName] && !r.CreatedOn.IsZero() {
				key := envMonthKey{env: r.TargetRefName, month: r.CreatedOn.Format("2006-01")}
				freqMap[key]++
			}
		}
	}
	deployFreq := make([]DeploymentFrequencyPoint, 0, len(freqMap))
	for k, count := range freqMap {
		deployFreq = append(deployFreq, DeploymentFrequencyPoint{
			Month:       k.month,
			Environment: k.env,
			Count:       count,
		})
	}
	sort.Slice(deployFreq, func(i, j int) bool {
		if deployFreq[i].Month != deployFreq[j].Month {
			return deployFreq[i].Month < deployFreq[j].Month
		}
		return deployFreq[i].Environment < deployFreq[j].Environment
	})

	return PipelineChartsResponse{
		Summary: PipelineSummary{
			TotalPipelines:     len(unique),
			SuccessCount:       successCount,
			FailedCount:        failedCount,
			ErrorCount:         errorCount,
			StoppedCount:       stoppedCount,
			AvgDurationSeconds: avgDuration,
			UniqueCreators:     len(creatorCountSet),
			UniqueRepos:        len(repoCountSet),
		},
		DeploymentFrequency: deployFreq,
		ResultsByRepo:       sortedResultsByRepo(mapPipelineResultsToSlice(byRepo)),
		PipelinesByCreator:  sortedByValue(mapToLabelValues(byCreator)),
		AvgDurationByRepo:   sortedByValue(avgDurByRepo),
		PipelinesByMonth:    sortedByLabel(mapToLabelValues(byMonth)),
	}
}

// --- helpers ---

func mapPipelineResultsToSlice(m map[string]*PipelineResultBreakdown) []PipelineResultBreakdown {
	out := make([]PipelineResultBreakdown, 0, len(m))
	for _, v := range m {
		out = append(out, *v)
	}
	return out
}

// sortedResultsByRepo sorts by total pipeline count descending so the repos
// with the most activity (and potentially the most failures) appear first.
func sortedResultsByRepo(s []PipelineResultBreakdown) []PipelineResultBreakdown {
	sort.Slice(s, func(i, j int) bool {
		totalI := s[i].Successful + s[i].Failed + s[i].Error + s[i].Stopped
		totalJ := s[j].Successful + s[j].Failed + s[j].Error + s[j].Stopped
		return totalI > totalJ
	})
	return s
}
