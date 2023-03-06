package format

import "math"

// the difference between predefined and builtin is predefined entities is storing in metadata and could be changed
const (
	PredefinedMetricIDCpuUsage       = -16384
	PredefinedMetricIDSystemUptime   = -16385
	PredefinedMetricIDProcessCreated = -16386
	PredefinedMetricIDProcessRunning = -16387
	PredefinedMetricIDProcessBlocked = -16388

	PredefinedMetricNameCpuUsage       = "cpu_usage"
	PredefinedMetricNameSystemUptime   = "system_uptime"
	PredefinedMetricNameProcessCreated = "system_process_created"
	PredefinedMetricNameProcessRunning = "system_process_running"
	PredefinedMetricNameProcessBlocked = "system_process_blocked"
)

var PredefinedMetrics = map[int32]*MetricMetaValue{
	PredefinedMetricIDCpuUsage: {
		Name:        PredefinedMetricNameCpuUsage,
		Kind:        MetricKindValue,
		Description: "",
		Tags: []MetricMetaTag{{
			Description: "host",
		}},
	},
}

func init() {
	for id, m := range PredefinedMetrics {
		m.MetricID = id
		m.Visible = true
		m.PreKeyFrom = math.MaxInt32 // allow writing, but not yet selecting
		m.Tags = append([]MetricMetaTag{{Description: "environment"}}, m.Tags...)
		m.Tags = append([]MetricMetaTag{{Description: "hostname"}}, m.Tags...)
		for len(m.Tags) < MaxTags {
			m.Tags = append(m.Tags, MetricMetaTag{Description: "-"})
		}
		for i, t := range m.Tags {
			if t.Description == "tag_id" {
				m.Tags[i].ValueComments = convertToValueComments(tagIDTag2TagID)
				m.Tags[i].Raw = true
				continue
			}
			if i == 0 { // env is not raw
				continue
			}
			if m.Tags[i].RawKind != "" {
				m.Tags[i].Raw = true
				continue
			}
			if m.Tags[i].Description == "-" && m.Tags[i].Name == "" {
				m.Tags[i].Raw = true
				continue
			}
			if m.Tags[i].IsMetric || m.Tags[i].ValueComments != nil {
				m.Tags[i].Raw = true
			}
		}
		_ = m.RestoreCachedInfo()
	}
}
