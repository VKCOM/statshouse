package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouseApi"
	"github.com/vkcom/statshouse/internal/format"
)

func Test_transformPointQueryShifts(t *testing.T) {
	var hourSec int64 = 3600
	t.Run("check timeshift handling", func(t *testing.T) {
		gotReq, err := transformPointQuery(tlstatshouseApi.QueryPoint{
			FieldsMask: 0,
			Version:    0,
			TopN:       5,
			MetricName: format.BuiltinMetricNameNumaEvents,
			TimeFrom:   hourSec * 48,
			TimeTo:     hourSec * 72,
			Function:   tlstatshouseApi.FnAvg(),
			TimeShift:  []int64{-hourSec * 24},
			What:       nil,
		}, format.BuiltinMetrics[format.BuiltinMetricIDNumaEvents])
		require.NoError(t, err)
		require.Len(t, gotReq.shifts, 2)
		require.Equal(t, gotReq.shifts[0], time.Second*time.Duration(-hourSec*24))
		require.Equal(t, gotReq.shifts[1], time.Duration(0))
	})

	t.Run("check timeshift handling", func(t *testing.T) {
		gotReq, err := transformPointQuery(tlstatshouseApi.QueryPoint{
			FieldsMask: 0,
			Version:    0,
			TopN:       5,
			MetricName: format.BuiltinMetricNameNumaEvents,
			TimeFrom:   hourSec * 48,
			TimeTo:     hourSec * 72,
			Function:   tlstatshouseApi.FnAvg(),
			TimeShift:  nil,
			What:       nil,
		}, format.BuiltinMetrics[format.BuiltinMetricIDNumaEvents])
		require.NoError(t, err)
		require.Len(t, gotReq.shifts, 1)
		require.Equal(t, gotReq.shifts[0], time.Duration(0))
	})
}
