package dashboard

import (
	"testing"
	"time"
)

// --- test helpers ---

func pipelineDate(s string) time.Time {
	t, _ := time.Parse("2006-01-02", s)
	return t
}

func makePipelineRow(uuid, repo, creator, stateName, resultName, envName, targetRef string, createdOn time.Time) PipelineRow {
	return PipelineRow{
		PipelineUUID:     uuid,
		Repo:             repo,
		Creator:          creator,
		StateName:        stateName,
		ResultName:       resultName,
		EnvironmentName:  envName,
		TargetRefName:    targetRef,
		DeploymentStatus: resultName, // mirror result into deployment status for test convenience
		CreatedOn:        createdOn,
		CompletedOn:      createdOn.Add(5 * time.Minute),
		DurationSeconds:  300,
	}
}

// --- filterPipelineRows ---

func TestFilterPipelineRows(t *testing.T) {
	rows := []PipelineRow{
		makePipelineRow("p1", "repo-a", "Alice", "COMPLETED", "SUCCESSFUL", "Production", "main", pipelineDate("2024-01-15")),
		makePipelineRow("p2", "repo-b", "Bob", "COMPLETED", "FAILED", "Staging", "develop", pipelineDate("2024-02-10")),
		makePipelineRow("p3", "repo-a", "Bot", "COMPLETED", "SUCCESSFUL", "Production", "main", pipelineDate("2024-03-05")),
		makePipelineRow("p4", "repo-b", "Alice", "COMPLETED", "ERROR", "", "main", pipelineDate("2024-01-20")),
	}
	bots := map[string]bool{"Bot": true}

	t.Run("no filters returns non-bot rows", func(t *testing.T) {
		got := filterPipelineRows(rows, PipelineFilterParams{}, bots)
		if len(got) != 3 {
			t.Errorf("want 3 rows, got %d", len(got))
		}
	})

	t.Run("repo filter", func(t *testing.T) {
		got := filterPipelineRows(rows, PipelineFilterParams{Repos: []string{"repo-a"}}, bots)
		// p1 from repo-a passes; p3 is bot; p4 is repo-b
		if len(got) != 1 {
			t.Errorf("want 1 row, got %d", len(got))
		}
		if got[0].PipelineUUID != "p1" {
			t.Errorf("want p1, got %q", got[0].PipelineUUID)
		}
	})

	t.Run("creator include filter", func(t *testing.T) {
		got := filterPipelineRows(rows, PipelineFilterParams{Creators: []string{"Alice"}}, bots)
		if len(got) != 2 {
			t.Errorf("want 2 rows (p1, p4), got %d", len(got))
		}
	})

	t.Run("exclude filter removes matching creator", func(t *testing.T) {
		got := filterPipelineRows(rows, PipelineFilterParams{ExcludeUsers: []string{"Bob"}}, bots)
		// p2 (Bob) excluded; Bot already excluded by bots map
		if len(got) != 2 {
			t.Errorf("want 2 rows, got %d", len(got))
		}
		for _, r := range got {
			if r.Creator == "Bob" {
				t.Error("Bob should be excluded")
			}
		}
	})

	t.Run("date range filter", func(t *testing.T) {
		got := filterPipelineRows(rows, PipelineFilterParams{
			DateFrom: pipelineDate("2024-02-01"),
			DateTo:   pipelineDate("2024-03-31"),
		}, bots)
		// p2: 2024-02-10 passes; p3 is bot; p1,p4 are before DateFrom
		if len(got) != 1 {
			t.Errorf("want 1 row, got %d", len(got))
		}
		if got[0].PipelineUUID != "p2" {
			t.Errorf("want p2, got %q", got[0].PipelineUUID)
		}
	})

	t.Run("environment filter", func(t *testing.T) {
		got := filterPipelineRows(rows, PipelineFilterParams{Environments: []string{"Staging"}}, bots)
		if len(got) != 1 || got[0].PipelineUUID != "p2" {
			t.Errorf("want only p2 (Staging), got %d rows", len(got))
		}
	})

	t.Run("result name filter", func(t *testing.T) {
		got := filterPipelineRows(rows, PipelineFilterParams{ResultNames: []string{"FAILED", "ERROR"}}, bots)
		if len(got) != 2 {
			t.Errorf("want 2 rows (p2, p4), got %d", len(got))
		}
	})
}

// --- deduplicatePipelineRows ---

func TestDeduplicatePipelineRows(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		if got := deduplicatePipelineRows(nil); got != nil {
			t.Errorf("want nil, got %v", got)
		}
	})

	t.Run("unique rows kept as-is", func(t *testing.T) {
		rows := []PipelineRow{
			{PipelineUUID: "p1", Repo: "r"},
			{PipelineUUID: "p2", Repo: "r"},
		}
		got := deduplicatePipelineRows(rows)
		if len(got) != 2 {
			t.Errorf("want 2, got %d", len(got))
		}
	})

	t.Run("duplicate pipeline UUIDs deduplicated, first wins", func(t *testing.T) {
		rows := []PipelineRow{
			{PipelineUUID: "p1", Repo: "r", EnvironmentName: "Production"},
			{PipelineUUID: "p1", Repo: "r", EnvironmentName: "Staging"},
			{PipelineUUID: "p2", Repo: "r", EnvironmentName: "Production"},
		}
		got := deduplicatePipelineRows(rows)
		if len(got) != 2 {
			t.Fatalf("want 2, got %d", len(got))
		}
		if got[0].EnvironmentName != "Production" {
			t.Errorf("first occurrence should win: want Production, got %q", got[0].EnvironmentName)
		}
	})
}

// --- BuildPipelineMeta ---

func TestBuildPipelineMeta(t *testing.T) {
	rows := []PipelineRow{
		makePipelineRow("p1", "repo-a", "Alice", "COMPLETED", "SUCCESSFUL", "Production", "main", pipelineDate("2024-01-01")),
		makePipelineRow("p2", "repo-b", "Bob", "COMPLETED", "FAILED", "Staging", "develop", pipelineDate("2024-06-15")),
		makePipelineRow("p3", "repo-a", "Bot", "COMPLETED", "SUCCESSFUL", "", "main", pipelineDate("2024-03-10")),
	}
	bots := map[string]bool{"Bot": true}
	excluded := []string{"Bot"}

	meta := BuildPipelineMeta(rows, bots, excluded, []string{"main"})

	t.Run("repos contains all repos", func(t *testing.T) {
		if len(meta.Repos) != 2 {
			t.Errorf("want 2 repos, got %d: %v", len(meta.Repos), meta.Repos)
		}
	})

	t.Run("creators excludes bots", func(t *testing.T) {
		for _, c := range meta.Creators {
			if c == "Bot" {
				t.Error("Bot should be excluded from creators")
			}
		}
		if len(meta.Creators) != 2 {
			t.Errorf("want 2 creators (Alice, Bob), got %d: %v", len(meta.Creators), meta.Creators)
		}
	})

	t.Run("environments excludes empty", func(t *testing.T) {
		for _, e := range meta.Environments {
			if e == "" {
				t.Error("empty environment should not appear")
			}
		}
		if len(meta.Environments) != 2 {
			t.Errorf("want 2 environments, got %d", len(meta.Environments))
		}
	})

	t.Run("targets returned", func(t *testing.T) {
		if len(meta.Targets) != 2 {
			t.Errorf("want 2 targets (main, develop), got %d", len(meta.Targets))
		}
	})

	t.Run("date bounds correct", func(t *testing.T) {
		if meta.DateMin != "2024-01-01" {
			t.Errorf("DateMin: want 2024-01-01, got %q", meta.DateMin)
		}
		if meta.DateMax != "2024-06-15" {
			t.Errorf("DateMax: want 2024-06-15, got %q", meta.DateMax)
		}
	})

	t.Run("excluded authors propagated", func(t *testing.T) {
		if len(meta.ExcludedAuthors) != 1 || meta.ExcludedAuthors[0] != "Bot" {
			t.Errorf("ExcludedAuthors: want [Bot], got %v", meta.ExcludedAuthors)
		}
	})

	t.Run("empty rows returns empty meta", func(t *testing.T) {
		m := BuildPipelineMeta(nil, nil, nil, nil)
		if m.DateMin != "" || m.DateMax != "" {
			t.Error("expected empty dates for nil rows")
		}
		if len(m.Repos) != 0 {
			t.Error("expected no repos")
		}
	})
}

// --- BuildPipelineCharts ---

func TestBuildPipelineCharts(t *testing.T) {
	jan := pipelineDate("2024-01-15")
	feb := pipelineDate("2024-02-10")

	rows := []PipelineRow{
		{PipelineUUID: "p1", Repo: "repo-a", Creator: "Alice", ResultName: "SUCCESSFUL",
			TargetRefName: "master", EnvironmentName: "Production", DeploymentStatus: "SUCCESSFUL",
			CreatedOn: jan, CompletedOn: jan.Add(5 * time.Minute), DurationSeconds: 300},
		{PipelineUUID: "p2", Repo: "repo-a", Creator: "Bob", ResultName: "FAILED",
			TargetRefName: "feature/x", EnvironmentName: "", DeploymentStatus: "",
			CreatedOn: jan, CompletedOn: jan.Add(2 * time.Minute), DurationSeconds: 120},
		{PipelineUUID: "p3", Repo: "repo-b", Creator: "Alice", ResultName: "SUCCESSFUL",
			TargetRefName: "master", EnvironmentName: "Staging", DeploymentStatus: "SUCCESSFUL",
			CreatedOn: feb, CompletedOn: feb.Add(3 * time.Minute), DurationSeconds: 180},
		// Duplicate pipeline row (same UUID) — should be deduplicated in frequency count.
		{PipelineUUID: "p1", Repo: "repo-a", Creator: "Alice", ResultName: "SUCCESSFUL",
			TargetRefName: "master", EnvironmentName: "Staging", DeploymentStatus: "SUCCESSFUL",
			CreatedOn: jan, CompletedOn: jan.Add(5 * time.Minute), DurationSeconds: 300},
	}

	charts := BuildPipelineCharts(rows, nil, PipelineFilterParams{ProductionRefs: []string{"master"}})

	t.Run("summary counts deduplicated pipelines", func(t *testing.T) {
		// p1 (2 env rows) deduplicates to 1; p2, p3 = 3 unique total
		if charts.Summary.TotalPipelines != 3 {
			t.Errorf("TotalPipelines: want 3, got %d", charts.Summary.TotalPipelines)
		}
		if charts.Summary.SuccessCount != 2 {
			t.Errorf("SuccessCount: want 2 (p1, p3), got %d", charts.Summary.SuccessCount)
		}
		if charts.Summary.FailedCount != 1 {
			t.Errorf("FailedCount: want 1 (p2), got %d", charts.Summary.FailedCount)
		}
	})

	t.Run("deployment frequency counts deduplicated successful runs on production branch", func(t *testing.T) {
		// p1 (master, SUCCESSFUL, jan) and p3 (master, SUCCESSFUL, feb) → 2 unique pipeline runs.
		// The duplicate p1 row is deduplicated → still 2 total.
		// ProductionRefs = ["master"], so feature/x (p2) is excluded.
		totalDeployments := 0
		for _, d := range charts.DeploymentFrequency {
			totalDeployments += d.Count
		}
		if totalDeployments != 2 {
			t.Errorf("DeploymentFrequency total count: want 2, got %d", totalDeployments)
		}
	})

	t.Run("results by repo sorted by total descending", func(t *testing.T) {
		if len(charts.ResultsByRepo) < 2 {
			t.Fatalf("want at least 2 repo results, got %d", len(charts.ResultsByRepo))
		}
		totalFirst := charts.ResultsByRepo[0].Successful + charts.ResultsByRepo[0].Failed
		totalSecond := charts.ResultsByRepo[1].Successful + charts.ResultsByRepo[1].Failed
		if totalFirst < totalSecond {
			t.Error("ResultsByRepo should be sorted descending by total")
		}
	})

	t.Run("bot exclusion", func(t *testing.T) {
		botRows := []PipelineRow{
			makePipelineRow("b1", "repo-a", "CI Bot", "COMPLETED", "SUCCESSFUL", "", "main", jan),
			makePipelineRow("p1", "repo-a", "Alice", "COMPLETED", "SUCCESSFUL", "", "main", jan),
		}
		bots := map[string]bool{"CI Bot": true}
		charts := BuildPipelineCharts(botRows, bots, PipelineFilterParams{})
		for _, lv := range charts.PipelinesByCreator {
			if lv.Label == "CI Bot" {
				t.Error("CI Bot should be excluded from PipelinesByCreator")
			}
		}
	})

	t.Run("creator include filter applies to Creator field", func(t *testing.T) {
		charts := BuildPipelineCharts(rows, nil, PipelineFilterParams{Creators: []string{"Alice"}})
		// Only Alice's pipelines: p1 (2 rows → 1 unique), p3 = 2 unique
		if charts.Summary.TotalPipelines != 2 {
			t.Errorf("TotalPipelines with creator filter: want 2, got %d", charts.Summary.TotalPipelines)
		}
	})

	t.Run("exclude filter removes matching creator", func(t *testing.T) {
		charts := BuildPipelineCharts(rows, nil, PipelineFilterParams{ExcludeUsers: []string{"Bob"}})
		// Excludes p2 (Bob): p1 + p3 = 2 unique
		if charts.Summary.TotalPipelines != 2 {
			t.Errorf("TotalPipelines with exclude filter: want 2, got %d", charts.Summary.TotalPipelines)
		}
	})

	t.Run("empty rows returns zero summary", func(t *testing.T) {
		charts := BuildPipelineCharts(nil, nil, PipelineFilterParams{})
		if charts.Summary.TotalPipelines != 0 {
			t.Errorf("want 0, got %d", charts.Summary.TotalPipelines)
		}
	})
}

// --- sortedResultsByRepo ---

func TestSortedResultsByRepo(t *testing.T) {
	input := []PipelineResultBreakdown{
		{Label: "small-repo", Successful: 1},
		{Label: "big-repo", Successful: 5, Failed: 3},
		{Label: "mid-repo", Successful: 2, Failed: 1},
	}
	got := sortedResultsByRepo(input)
	if got[0].Label != "big-repo" {
		t.Errorf("first should be big-repo (total 8), got %q", got[0].Label)
	}
	if got[1].Label != "mid-repo" {
		t.Errorf("second should be mid-repo (total 3), got %q", got[1].Label)
	}
}
