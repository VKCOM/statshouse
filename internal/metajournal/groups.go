package metajournal

// use separete wrapper, because we need to handle external metrics and builtin metrics
type GroupMetricsStorage struct {
}

func (ms *MetricsStorage) GetGroupWithMetricsList(id int32) (GroupWithMetricsList, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var metricNames []string
	if group, ok := ms.groupsByID[id]; ok {
		if metrics, ok := ms.metricsByGroup[group.ID]; ok {
			for _, metric := range metrics {
				metricNames = append(metricNames, metric.Name)
			}
		}
		return GroupWithMetricsList{Group: group, Metrics: metricNames}, true
	}
	return GroupWithMetricsList{}, false
}
