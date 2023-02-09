// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"net/http"
	"strings"

	"github.com/vkcom/statshouse/internal/vkgo/vkuth"

	"github.com/vkcom/statshouse/internal/format"
)

type accessManager struct {
	getGroupByMetricName func(string) *format.MetricsGroup
}

type accessInfo struct {
	user                 string
	insecureMode         bool // full access to everything; can not be obtained from bits
	protectedPrefixes    []string
	bitAdmin             bool
	bitViewDefault       bool
	bitEditDefault       bool
	bitViewPrefix        map[string]bool
	bitEditPrefix        map[string]bool
	bitViewMetric        map[string]bool
	bitEditMetric        map[string]bool
	skipBadgesValidation bool
	accessManager        *accessManager
}

func (m *accessManager) parseAccessToken(jwtHelper *vkuth.JWTHelper,
	accessToken string,
	protectedPrefixes []string,
	localMode bool,
	insecureMode bool) (accessInfo, error) {
	if localMode || insecureMode {
		ai := accessInfo{
			protectedPrefixes: protectedPrefixes,
			bitViewDefault:    true,
			bitEditDefault:    true,
			bitViewPrefix:     map[string]bool{},
			bitEditPrefix:     map[string]bool{},
			bitViewMetric:     map[string]bool{},
			bitEditMetric:     map[string]bool{},
			insecureMode:      insecureMode,
			accessManager:     m,
		}
		return ai, nil
	}
	data, err := jwtHelper.ParseVkuthData(accessToken)
	if err != nil {
		return accessInfo{}, httpErr(http.StatusUnauthorized, err)
	}
	ai := accessInfo{
		user:              data.User,
		protectedPrefixes: protectedPrefixes,
		bitViewPrefix:     map[string]bool{},
		bitEditPrefix:     map[string]bool{},
		bitViewMetric:     map[string]bool{},
		bitEditMetric:     map[string]bool{},
		insecureMode:      insecureMode,
		accessManager:     m,
	}

	bits := data.Bits
	for b := range bits {
		switch {
		case b == "admin":
			ai.bitAdmin = true
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
		}
	}

	return ai, nil
}

func (ai *accessInfo) protectedMetric(metric string) bool {
	group := ai.accessManager.getGroupByMetricName(metric)
	if group != nil {
		return group.Protected
	}
	// todo remove
	for _, p := range ai.protectedPrefixes {
		if strings.HasPrefix(metric, p) {
			return true
		}
	}
	return false
}

func (ai *accessInfo) canViewMetric(metric string) bool {
	if metric == format.BuiltinMetricNameBadges && ai.skipBadgesValidation {
		return true
	}
	if ai.insecureMode {
		return true
	}

	return ai.bitViewMetric[metric] ||
		hasPrefixAccess(ai.bitViewPrefix, metric) ||
		(ai.bitViewDefault && !ai.protectedMetric(metric))
}

func (ai *accessInfo) canChangeMetricByName(create bool, oldName, newName string) bool {
	if ai.insecureMode || ai.bitAdmin {
		return true
	}

	if !create {
		oldGroup := ai.accessManager.getGroupByMetricName(oldName)
		newGroup := ai.accessManager.getGroupByMetricName(newName)
		if oldGroup != nil && newGroup != nil {
			if oldGroup.ID != newGroup.ID {
				return false
			}
		} else if oldGroup != newGroup {
			return false
		}
	}

	// we expect that oldName and newName both are in the same group
	return ai.bitEditMetric[oldName] && ai.bitEditMetric[newName] ||
		(hasPrefixAccess(ai.bitEditPrefix, oldName) && hasPrefixAccess(ai.bitEditPrefix, newName)) ||
		(ai.bitEditDefault && !ai.protectedMetric(oldName) && !ai.protectedMetric(newName))
}

func (ai *accessInfo) canEditMetric(create bool, old format.MetricMetaValue, new_ format.MetricMetaValue) bool {
	if ai.insecureMode {
		return true
	}
	if ai.canChangeMetricByName(create, old.Name, new_.Name) {
		if ai.bitAdmin {
			return true
		}
		if old.Weight != new_.Weight && !(old.Weight == 0 && new_.Weight == 1) {
			return false
		}
		if hasPreKey(old) != hasPreKey(new_) {
			return false
		}
		return true
	}
	return false
}

func (ai *accessInfo) isAdmin() bool {
	if ai.insecureMode {
		return true
	}
	return ai.bitAdmin
}

func (ai *accessInfo) withBadgesRequest() accessInfo {
	return accessInfo{
		user:                 ai.user,
		insecureMode:         ai.insecureMode,
		protectedPrefixes:    ai.protectedPrefixes,
		bitAdmin:             ai.bitAdmin,
		bitViewPrefix:        ai.bitViewPrefix,
		bitEditPrefix:        ai.bitEditPrefix,
		bitViewMetric:        ai.bitViewMetric,
		bitEditMetric:        ai.bitEditMetric,
		skipBadgesValidation: true,
	}
}

func hasPreKey(m format.MetricMetaValue) bool {
	return m.PreKeyTagID != "" || m.PreKeyFrom != 0
}

func hasPrefixAccess(m map[string]bool, metric string) bool {
	for prefix := range m {
		if strings.HasPrefix(metric, prefix) {
			return true
		}
	}
	return false
}
