// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/vkuth"
)

type AccessInfo struct {
	User                 string
	Service              bool
	InsecureMode         bool // full access to everything; can not be obtained from bits
	protectedPrefixes    []string
	BitAdmin             bool
	bitDeveloper         bool
	bitViewDefault       bool
	bitEditDefault       bool
	bitViewPrefix        map[string]bool
	bitEditPrefix        map[string]bool
	bitViewMetric        map[string]bool
	bitEditMetric        map[string]bool
	SkipBadgesValidation bool
}

func ParseAccessToken(jwtHelper *vkuth.JWTHelper,
	accessToken string,
	protectedPrefixes []string,
	localMode bool,
	insecureMode bool) (AccessInfo, error) {
	if localMode || insecureMode {
		ai := AccessInfo{
			User:              "@local_mode",
			protectedPrefixes: protectedPrefixes,
			bitViewDefault:    true,
			bitEditDefault:    true,
			bitDeveloper:      localMode,
			bitViewPrefix:     map[string]bool{},
			bitEditPrefix:     map[string]bool{},
			bitViewMetric:     map[string]bool{},
			bitEditMetric:     map[string]bool{},
			InsecureMode:      insecureMode,
		}
		return ai, nil
	}
	data, err := jwtHelper.ParseVkuthData(accessToken)
	if err != nil {
		return AccessInfo{}, HttpErr(http.StatusUnauthorized, err)
	}
	ai := AccessInfo{
		User:              data.User,
		Service:           data.IsService,
		protectedPrefixes: protectedPrefixes,
		bitViewPrefix:     map[string]bool{},
		bitEditPrefix:     map[string]bool{},
		bitViewMetric:     map[string]bool{},
		bitEditMetric:     map[string]bool{},
		InsecureMode:      insecureMode,
	}

	bits := data.Bits
	for b := range bits {
		switch {
		case b == "admin":
			ai.BitAdmin = true
		case b == "developer":
			ai.bitDeveloper = true
		case b == "view_default":
			ai.bitViewDefault = true
		case b == "edit_default":
			ai.bitEditDefault = true
		case strings.HasPrefix(b, "view_prefix."):
			ai.bitViewPrefix[b[len("view_prefix."):]] = true
		case strings.HasPrefix(b, "edit_prefix."):
			ai.bitEditPrefix[b[len("edit_prefix."):]] = true
		case strings.HasPrefix(b, "view_metric."):
			ai.bitViewMetric[b[len("view_metric."):]] = true
		case strings.HasPrefix(b, "edit_metric."):
			ai.bitEditMetric[b[len("edit_metric."):]] = true
		// use another bit, because vkuth token can't contain ':'
		case strings.HasPrefix(b, "view_namespace."):
			ai.bitViewPrefix[b[len("view_namespace."):]+":"] = true
		case strings.HasPrefix(b, "edit_namespace."):
			ai.bitEditPrefix[b[len("edit_namespace."):]+":"] = true
		}
	}

	return ai, nil
}

func (ai *AccessInfo) ToMetadata() string {
	m := Metadata{
		UserEmail: ai.User,
		UserName:  "",
		UserRef:   "",
	}
	res, _ := json.Marshal(&m)
	return string(res)
}

func (ai *AccessInfo) ProtectedMetric(name string) bool {
	for _, p := range ai.protectedPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func (ai *AccessInfo) CanViewMetricName(name string) bool {
	if name == format.BuiltinMetricNameBadges && ai.SkipBadgesValidation {
		return true
	}
	if ai.InsecureMode {
		return true
	}
	if data_model.RemoteConfigMetric(name) && !ai.BitAdmin {
		return false // remote config can only be viewed by administrators
	}
	return ai.bitViewMetric[name] ||
		HasPrefixAccess(ai.bitViewPrefix, name) ||
		(ai.bitViewDefault && !ai.ProtectedMetric(name))
}

func (ai *AccessInfo) CanViewMetric(metric format.MetricMetaValue) bool {
	return ai.CanViewMetricName(metric.Name)
}

func (ai *AccessInfo) CanChangeMetricByName(create bool, old format.MetricMetaValue, new_ format.MetricMetaValue) bool {
	if ai.InsecureMode || ai.BitAdmin {
		return true
	}

	oldName := old.Name
	newName := new_.Name

	if data_model.RemoteConfigMetric(oldName) || data_model.RemoteConfigMetric(newName) {
		return false // remote config can only be set by administrators
	}
	return ai.bitEditMetric[oldName] && ai.bitEditMetric[newName] ||
		(HasPrefixAccess(ai.bitEditPrefix, oldName) && HasPrefixAccess(ai.bitEditPrefix, newName)) ||
		(ai.bitEditDefault && !ai.ProtectedMetric(oldName) && !ai.ProtectedMetric(newName))
}

func (ai *AccessInfo) CanEditMetric(create bool, old format.MetricMetaValue, new_ format.MetricMetaValue) bool {
	if ai.InsecureMode {
		return true
	}
	if ai.CanChangeMetricByName(create, old, new_) {
		if ai.BitAdmin {
			return true
		}
		if old.Weight != new_.Weight && !(old.Weight == 0 && new_.Weight == 1) {
			return false
		}
		if PreKey(old) != PreKey(new_) {
			return false
		}
		if PreKeyOnly(old) != PreKeyOnly(new_) {
			return false
		}
		if Skips(old) != Skips(new_) {
			return false
		}

		return true
	}
	return false
}

func (ai *AccessInfo) IsAdmin() bool {
	if ai.InsecureMode {
		return true
	}
	return ai.BitAdmin
}

func PreKey(m format.MetricMetaValue) uint32 {
	return m.PreKeyFrom
}

func PreKeyOnly(m format.MetricMetaValue) bool {
	return m.PreKeyOnly
}

func Skips(m format.MetricMetaValue) [3]bool {
	return [3]bool{m.SkipMaxHost, m.SkipMinHost, m.SkipSumSquare}
}

func HasPrefixAccess(m map[string]bool, metric string) bool {
	for prefix := range m {
		if strings.HasPrefix(metric, prefix) {
			return true
		}
	}
	return false
}
