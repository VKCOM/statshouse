package api

import (
	"fmt"
	"math"
	"testing"

	"github.com/mailru/easyjson/jwriter"
	"github.com/stretchr/testify/require"
)

func Test_NAN_Is_Null(t *testing.T) {
	t.Run("queryTableRow", func(t *testing.T) {
		r := queryTableRow{}
		r.Data = []float64{math.NaN(), 0}
		var jw jwriter.Writer
		r.MarshalEasyJSON(&jw)
		b, _ := jw.BuildBytes(nil)
		require.Equal(t, `{"time":0,"data":[null,0],"tags":null}`, string(b))
	})
	t.Run("SeriesResponse", func(t *testing.T) {
		r := SeriesResponse{}
		data := []float64{math.NaN(), 0}
		r.Series.SeriesData = append(r.Series.SeriesData, &data)
		var jw jwriter.Writer
		r.MarshalEasyJSON(&jw)
		b, _ := jw.BuildBytes(nil)
		require.Equal(t, `{"series":{"time":null,"series_meta":null,"series_data":[[null,0]]},"sampling_factor_src":0,"sampling_factor_agg":0,"receive_errors":0,"receive_warnings":0,"mapping_errors":0,"promql":"","__debug_queries":null,"excess_point_left":false,"excess_point_right":false,"metric":null}`, string(b))
	})

	t.Run("GetPointResp", func(t *testing.T) {
		r := GetPointResp{}
		r.PointData = []float64{math.NaN(), 0}
		var jw jwriter.Writer
		r.MarshalEasyJSON(&jw)
		b, _ := jw.BuildBytes(nil)
		fmt.Println(string(b))
		require.Equal(t, `{"point_meta":null,"point_data":[null,0],"__debug_queries":null}`, string(b))
	})
}
