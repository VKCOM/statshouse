// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package env

import (
	"os"
)

var (
	metricNamePrefix = os.Getenv("COMMON_METRICS_NAME_PREFIX")
)

func FullMetricName(metric string) string {
	return metricNamePrefix + metric
}
