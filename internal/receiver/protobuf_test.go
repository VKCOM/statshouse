// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/stretchr/testify/require"

	"pgregory.net/rapid"

	"google.golang.org/protobuf/proto"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/receiver/pb"
)

func TestProtobuf(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var src pb.MetricBatch
		var dst tlstatshouse.AddMetricsBatchBytes

		num := rapid.IntRange(0, 100).Draw(t, "num_metrics")
		for i := 0; i < num; i++ {
			metric := &pb.Metric{
				Name:    rapid.String().Draw(t, "name"),
				Tags:    map[string]string{},
				Counter: rapid.Float64().Draw(t, "counter"),
				Ts:      rapid.Uint32().Draw(t, "timestamp"),
				Value:   rapid.SliceOf(rapid.Float64()).Draw(t, "value"),
				Unique:  rapid.SliceOf(rapid.Int64()).Draw(t, "unique"),
			}
			numHistogram := rapid.IntRange(0, 3).Draw(t, "num_histogram")
			for j := 0; j < numHistogram; j++ {
				centroid := &pb.Centroid{
					Value: rapid.Float64().Draw(t, "centroid_value"),
					Count: rapid.Float64().Draw(t, "centroid_counter"),
				}
				metric.Histogram = append(metric.Histogram, centroid)
			}

			numTags := rapid.IntRange(0, 20).Draw(t, "num_tags")
			for j := 0; j < numTags; j++ {
				metric.Tags[rapid.String().Draw(t, "key")] = rapid.String().Draw(t, "value")
			}
			src.Metrics = append(src.Metrics, metric)
		}

		out, err := proto.Marshal(&src)
		require.NoError(t, err)

		_, err = protobufUnmarshalStatshouseAddMetricBatch(&dst, out)
		require.NoError(t, err)

		require.Equal(t, len(src.Metrics), len(dst.Metrics))
		for i := 0; i < num; i++ {
			require.Equal(t, src.Metrics[i].Name, string(dst.Metrics[i].Name))
			require.Equal(t, src.Metrics[i].Counter, dst.Metrics[i].Counter)
			require.Equal(t, src.Metrics[i].Ts, dst.Metrics[i].Ts)
			require.True(t, cmp.Equal(src.Metrics[i].Value, dst.Metrics[i].Value, cmpopts.EquateEmpty()))
			require.True(t, cmp.Equal(src.Metrics[i].Unique, dst.Metrics[i].Unique, cmpopts.EquateEmpty()))
			for j, s := range src.Metrics[i].Histogram {
				d := dst.Metrics[i].Histogram[j]
				require.Equal(t, s.Value, d[0])
				require.Equal(t, s.Count, d[1])
			}

			dict := map[string]string{}
			for _, e := range dst.Metrics[i].Tags {
				dict[string(e.Key)] = string(e.Value)
			}
			require.Equal(t, src.Metrics[i].Tags, dict)
		}

		// var jsonBuf bytes.Buffer
		// err = dst.WriteJSON(&jsonBuf)
		// require.NoError(t, err)
		// if len(dst.Metrics) == 1 && len(dst.Metrics[0].Tags) != 0 {
		//	log.Printf("%s", jsonBuf.Bytes())
		// }
	})
}
