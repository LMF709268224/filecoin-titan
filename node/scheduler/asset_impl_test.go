package scheduler

import (
	"context"
	"testing"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
)

func TestGetAssetRecordsRejectsInvalidServerID(t *testing.T) {
	s := &Scheduler{}
	_, err := s.GetAssetRecords(
		context.Background(),
		10,
		0,
		[]string{"SeedSelect"},
		"550e8400-e29b-41d4-a716-446655440000 JOIN asset_record",
	)
	if err == nil {
		t.Fatal("GetAssetRecords expected an error for an invalid serverID")
	}
}

func TestNormalizeServerID(t *testing.T) {
	const canonical = "550e8400-e29b-41d4-a716-446655440000"

	tests := []struct {
		name    string
		input   dtypes.ServerID
		want    dtypes.ServerID
		wantErr bool
	}{
		{name: "canonical UUID", input: canonical, want: canonical},
		{name: "UUID without hyphens", input: "550e8400e29b41d4a716446655440000", want: canonical},
		{name: "invalid value", input: "not-a-uuid", wantErr: true},
		{name: "SQL syntax", input: "550e8400-e29b-41d4-a716-446655440000 JOIN asset_record", wantErr: true},
		{name: "SQL comment", input: "550e8400-e29b-41d4-a716-446655440000/**/JOIN/**/asset_record", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeServerID(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("normalizeServerID(%q) expected an error", tt.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("normalizeServerID(%q) returned an error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("normalizeServerID(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
