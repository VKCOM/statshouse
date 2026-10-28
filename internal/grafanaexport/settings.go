// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Package grafanaexport converts a StatsHouse dashboard into a Grafana dashboard
// that queries VictoriaMetrics. The conversion is deterministic and best-effort:
// metric names are used verbatim, nothing is looked up in VictoriaMetrics, and
// whatever cannot be translated is reported in Result.Warnings.
package grafanaexport

const (
	// Defaults for Input.Datasource, real values come from the API remote config.
	DefaultDatasourceType = "victoriametrics-datasource" // NOTE: optional non-sensitive marker
	DefaultDatasourceUID  = "DATASOURCE_UID_PLACEHOLDER"

	// Grafana 11.6.
	schemaVersion = 41

	rateWindow  = "$__rate_interval"
	gridColumns = 24
)
