package tstorage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOpenDiskPartition(t *testing.T) {
	tests := []struct {
		name      string
		dirPath   string
		retention time.Duration
		want      partition[float64]
		wantErr   bool
	}{
		{
			name:      "empty dir name given",
			dirPath:   "",
			retention: 24 * time.Hour,
			wantErr:   true,
		},
		{
			name:      "non-existent dir given",
			dirPath:   "./non-existent-dir",
			retention: 24 * time.Hour,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := openDiskPartition[float64](tt.dirPath, tt.retention)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.want, got)
		})
	}
}
