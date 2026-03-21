package util

import (
	"testing"
)

// --- computeDuration ---

func TestComputeDuration(t *testing.T) {
	cases := []struct {
		name             string
		createdOn        string
		completedOn      string
		buildSecondsUsed int
		want             int
	}{
		{
			name:        "valid timestamps: uses wall-clock difference",
			createdOn:   "2024-03-01T10:00:00Z",
			completedOn: "2024-03-01T10:05:00Z",
			want:        300,
		},
		{
			name:             "missing completedOn: falls back to buildSecondsUsed",
			createdOn:        "2024-03-01T10:00:00Z",
			completedOn:      "",
			buildSecondsUsed: 120,
			want:             120,
		},
		{
			name:             "both empty: returns zero",
			createdOn:        "",
			completedOn:      "",
			buildSecondsUsed: 0,
			want:             0,
		},
		{
			name:             "invalid timestamp format: falls back to buildSecondsUsed",
			createdOn:        "not-a-date",
			completedOn:      "also-not-a-date",
			buildSecondsUsed: 60,
			want:             60,
		},
		{
			name:             "negative duration (completedOn before createdOn): falls back",
			createdOn:        "2024-03-01T10:05:00Z",
			completedOn:      "2024-03-01T10:00:00Z",
			buildSecondsUsed: 42,
			want:             42,
		},
		{
			name:        "sub-minute duration parsed correctly",
			createdOn:   "2024-03-01T10:00:00Z",
			completedOn: "2024-03-01T10:00:45Z",
			want:        45,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := computeDuration(tc.createdOn, tc.completedOn, tc.buildSecondsUsed)
			if got != tc.want {
				t.Errorf("computeDuration(%q, %q, %d) = %d, want %d",
					tc.createdOn, tc.completedOn, tc.buildSecondsUsed, got, tc.want)
			}
		})
	}
}
