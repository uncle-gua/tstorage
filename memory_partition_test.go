package tstorage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_memoryPartition_InsertRows(t *testing.T) {
	tests := []struct {
		name               string
		memoryPartition    *memoryPartition[float64]
		rows               []Row[float64]
		wantErr            bool
		wantDataPoints     []*DataPoint[float64]
		wantOutOfOrderRows []Row[float64]
	}{
		{
			name:            "insert in-order rows",
			memoryPartition: newMemoryPartition[float64](nil, 0, "").(*memoryPartition[float64]),
			rows: []Row[float64]{
				{Metric: "metric1", DataPoint: DataPoint[float64]{Timestamp: 1, Value: 0.1}},
				{Metric: "metric1", DataPoint: DataPoint[float64]{Timestamp: 2, Value: 0.1}},
				{Metric: "metric1", DataPoint: DataPoint[float64]{Timestamp: 3, Value: 0.1}},
			},
			wantDataPoints: []*DataPoint[float64]{
				{Timestamp: 1, Value: 0.1},
				{Timestamp: 2, Value: 0.1},
				{Timestamp: 3, Value: 0.1},
			},
			wantOutOfOrderRows: []Row[float64]{},
		},
		{
			name: "insert out-of-order rows",
			memoryPartition: func() *memoryPartition[float64] {
				m := newMemoryPartition[float64](nil, 0, "").(*memoryPartition[float64])
				m.insertRows([]Row[float64]{
					{Metric: "metric1", DataPoint: DataPoint[float64]{Timestamp: 2, Value: 0.1}},
				})
				return m
			}(),
			rows: []Row[float64]{
				{Metric: "metric1", DataPoint: DataPoint[float64]{Timestamp: 1, Value: 0.1}},
			},
			wantDataPoints: []*DataPoint[float64]{
				{Timestamp: 2, Value: 0.1},
			},
			wantOutOfOrderRows: []Row[float64]{
				{Metric: "metric1", DataPoint: DataPoint[float64]{Timestamp: 1, Value: 0.1}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOutOfOrder, err := tt.memoryPartition.insertRows(tt.rows)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantOutOfOrderRows, gotOutOfOrder)

			got, _ := tt.memoryPartition.selectDataPoints("metric1", nil, 0, 4)
			assert.Equal(t, tt.wantDataPoints, got)
		})
	}
}

func Test_memoryPartition_SelectDataPoints(t *testing.T) {
	tests := []struct {
		name            string
		metric          string
		labels          []Label
		start           int64
		end             int64
		memoryPartition *memoryPartition[float64]
		want            []*DataPoint[float64]
	}{
		{
			name:            "given non-exist metric name",
			metric:          "unknown",
			start:           1,
			end:             2,
			memoryPartition: newMemoryPartition[float64](nil, 0, "").(*memoryPartition[float64]),
			want:            []*DataPoint[float64]{},
		},
		{
			name:   "select some points",
			metric: "metric1",
			start:  2,
			end:    4,
			memoryPartition: func() *memoryPartition[float64] {
				m := newMemoryPartition[float64](nil, 0, "").(*memoryPartition[float64])
				m.insertRows([]Row[float64]{
					{
						Metric:    "metric1",
						DataPoint: DataPoint[float64]{Timestamp: 1, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint[float64]{Timestamp: 2, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint[float64]{Timestamp: 3, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint[float64]{Timestamp: 4, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint[float64]{Timestamp: 5, Value: 0.1},
					},
				})
				return m
			}(),
			want: []*DataPoint[float64]{
				{Timestamp: 2, Value: 0.1},
				{Timestamp: 3, Value: 0.1},
			},
		},
		{
			name:   "select all points",
			metric: "metric1",
			start:  1,
			end:    4,
			memoryPartition: func() *memoryPartition[float64] {
				m := newMemoryPartition[float64](nil, 0, "").(*memoryPartition[float64])
				m.insertRows([]Row[float64]{
					{
						Metric:    "metric1",
						DataPoint: DataPoint[float64]{Timestamp: 1, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint[float64]{Timestamp: 2, Value: 0.1},
					},
					{
						Metric:    "metric1",
						DataPoint: DataPoint[float64]{Timestamp: 3, Value: 0.1},
					},
				})
				return m
			}(),
			want: []*DataPoint[float64]{
				{Timestamp: 1, Value: 0.1},
				{Timestamp: 2, Value: 0.1},
				{Timestamp: 3, Value: 0.1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := tt.memoryPartition.selectDataPoints(tt.metric, tt.labels, tt.start, tt.end)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_memoryMetric_EncodeAllPoints_sorted(t *testing.T) {
	mt := memoryMetric[float64]{
		points: []*DataPoint[float64]{
			{Timestamp: 1, Value: 0.1},
			{Timestamp: 3, Value: 0.1},
		},
		outOfOrderPoints: []*DataPoint[float64]{
			{Timestamp: 4, Value: 0.1},
			{Timestamp: 2, Value: 0.1},
		},
	}
	allTimestamps := make([]int64, 0, 4)
	encoder := fakeEncoder[float64]{
		encodePointFunc: func(p *DataPoint[float64]) error {
			allTimestamps = append(allTimestamps, p.Timestamp)
			return nil
		},
	}
	err := mt.encodeAllPoints(&encoder)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3, 4}, allTimestamps)
}

func Test_memoryMetric_EncodeAllPoints_error(t *testing.T) {
	mt := memoryMetric[float64]{
		points: []*DataPoint[float64]{{Timestamp: 1, Value: 0.1}},
	}
	encoder := fakeEncoder[float64]{
		encodePointFunc: func(p *DataPoint[float64]) error {
			return fmt.Errorf("some error")
		},
	}
	err := mt.encodeAllPoints(&encoder)
	assert.Error(t, err)
}

func Test_toUnix(t *testing.T) {
	tests := []struct {
		name      string
		t         time.Time
		precision TimestampPrecision
		want      int64
	}{
		{
			name:      "to nanosecond",
			t:         time.Unix(1600000000, 0),
			precision: Nanoseconds,
			want:      1600000000000000000,
		},
		{
			name:      "to microsecond",
			t:         time.Unix(1600000000, 0),
			precision: Microseconds,
			want:      1600000000000000,
		},
		{
			name:      "to millisecond",
			t:         time.Unix(1600000000, 0),
			precision: Milliseconds,
			want:      1600000000000,
		},
		{
			name:      "to second",
			t:         time.Unix(1600000000, 0),
			precision: Seconds,
			want:      1600000000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toUnix(tt.t, tt.precision)
			assert.Equal(t, tt.want, got)
		})
	}
}
