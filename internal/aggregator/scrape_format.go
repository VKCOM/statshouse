// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"encoding/json"
	"fmt"

	"github.com/VKCOM/statshouse/internal/format"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/relabel"
)

type ScrapeConfig struct {
	Options       scrapeOptions      `json:"statshouse,omitempty"`
	GlobalConfig  scrapeGlobalConfig `json:"global,omitempty"`
	ScrapeConfigs []scrapeJobConfig  `json:"scrape_configs,omitempty"`
}

type scrapeOptions struct {
	Namespace    string   `json:"namespace"`
	GaugeMetrics []string `json:"gauge_metrics,omitempty"`

	NamespaceID int32 `json:"-"`
}

type scrapeGlobalConfig struct {
	ScrapeInterval model.Duration `json:"scrape_interval,omitempty"`
	ScrapeTimeout  model.Duration `json:"scrape_timeout,omitempty"`
}

type scrapeJobConfig struct {
	JobName              string                `json:"job_name"`
	ScrapeInterval       model.Duration        `json:"scrape_interval,omitempty"`
	ScrapeTimeout        model.Duration        `json:"scrape_timeout,omitempty"`
	MetricsPath          string                `json:"metrics_path,omitempty"`
	Scheme               string                `json:"scheme,omitempty"`
	RelabelConfigs       []scrapeRelableConfig `json:"relabel_configs,omitempty"`
	MetricRelabelConfigs []scrapeRelableConfig `json:"metric_relabel_configs,omitempty"`

	StaticConfigs []scrapeGroupConfig  `json:"static_configs,omitempty"`
	ConsulConfigs []scrapeConsulConfig `json:"consul_sd_configs,omitempty"`
}

type scrapeConsulConfig struct {
	Server     string `json:"server,omitempty"`
	Token      string `json:"token,omitempty"`
	Datacenter string `json:"datacenter,omitempty"`
}

type scrapeGroupConfig struct {
	Targets []string       `json:"targets,omitempty"`
	Labels  model.LabelSet `json:"labels,omitempty"`
}

type scrapeRelableConfig struct {
	SourceLabels model.LabelNames `json:"source_labels,omitempty"`
	Separator    string           `json:"separator,omitempty"`
	Regex        string           `json:"regex,omitempty"`
	Modulus      uint64           `json:"modulus,omitempty"`
	TargetLabel  string           `json:"target_label,omitempty"`
	Replacement  string           `json:"replacement,omitempty"`
	Action       string           `json:"action,omitempty"`
}

func DeserializeScrapeConfig(b []byte, m format.MetaStorageInterface) ([]ScrapeConfig, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var res []ScrapeConfig
	err := json.Unmarshal(b, &res)
	if err != nil {
		res = make([]ScrapeConfig, 1)
		err = json.Unmarshal(b, &res[0])
		if err != nil {
			return nil, err
		}
	}
	for i := range res {
		c := &res[i]
		if c.Options.Namespace == "" {
			return nil, fmt.Errorf("scrape namespace not set")
		}
		if m != nil {
			namespace := m.GetNamespaceByName(c.Options.Namespace)
			if namespace == nil {
				return nil, fmt.Errorf("scrape namespace not found %q", c.Options.Namespace)
			}
			if namespace.ID == format.BuiltinNamespaceIDDefault {
				return nil, fmt.Errorf("scrape namespace can not be __default")
			}
			c.Options.NamespaceID = namespace.ID
		}
		if c.GlobalConfig.ScrapeInterval == 0 {
			c.GlobalConfig.ScrapeInterval = config.DefaultGlobalConfig.ScrapeInterval
		}
		if c.GlobalConfig.ScrapeTimeout == 0 {
			c.GlobalConfig.ScrapeTimeout = config.DefaultGlobalConfig.ScrapeTimeout
		}
		for _, item := range c.ScrapeConfigs {
			if item.ScrapeInterval == 0 {
				item.ScrapeInterval = c.GlobalConfig.ScrapeInterval
			}
			if item.ScrapeTimeout == 0 {
				if c.GlobalConfig.ScrapeTimeout > item.ScrapeInterval {
					item.ScrapeTimeout = item.ScrapeInterval
				} else {
					item.ScrapeTimeout = c.GlobalConfig.ScrapeTimeout
				}
			}
		}
	}
	return res, nil
}

func (j *scrapeJobConfig) UnmarshalJSON(s []byte) error {
	type alias scrapeJobConfig
	j.MetricsPath = config.DefaultScrapeConfig.MetricsPath
	j.Scheme = config.DefaultScrapeConfig.Scheme
	return json.Unmarshal(s, (*alias)(j))
}

func (src *scrapeRelableConfig) toPrometheusFormat() (relabel.Config, error) {
	var regex relabel.Regexp
	if src.Regex != "" {
		var err error
		if regex, err = relabel.NewRegexp(src.Regex); err != nil {
			return relabel.Config{}, err
		}
	}
	res := relabel.DefaultRelabelConfig // copy
	if len(src.SourceLabels) != 0 {
		res.SourceLabels = src.SourceLabels
	}
	if len(src.Separator) != 0 {
		res.Separator = src.Separator
	}
	if src.Regex != "" {
		res.Regex = regex
	}
	res.Modulus = src.Modulus
	if src.TargetLabel != "" {
		res.TargetLabel = src.TargetLabel
	}
	if src.Replacement != "" {
		res.Replacement = src.Replacement
	}
	if src.Action != "" {
		res.Action = relabel.Action(src.Action)
	}
	return res, nil
}
