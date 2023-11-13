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

type accessInfo struct {
	user                 string
	service              bool
	insecureMode         bool // full access to everything; can not be obtained from bits
	protectedPrefixes    []string
	bitAdmin             bool
	bitDeveloper         bool
	bitViewDefault       bool
	bitEditDefault       bool
	bitViewPrefix        map[string]bool
	bitEditPrefix        map[string]bool
	bitViewMetric        map[string]bool
	bitEditMetric        map[string]bool
	skipBadgesValidation bool
}

func parseAccessToken(jwtHelper *vkuth.JWTHelper,
	accessToken string,
	protectedPrefixes []string,
	localMode bool,
	insecureMode bool) (accessInfo, error) {
	if localMode || insecureMode {
		ai := accessInfo{
			protectedPrefixes: protectedPrefixes,
			bitViewDefault:    true,
			bitEditDefault:    true,
			bitDeveloper:      localMode,
			bitViewPrefix:     map[string]bool{},
			bitEditPrefix:     map[string]bool{},
			bitViewMetric:     map[string]bool{},
			bitEditMetric:     map[string]bool{},
			insecureMode:      insecureMode,
		}
		return ai, nil
	}
	data, err := jwtHelper.ParseVkuthData(accessToken)
	if err != nil {
		return accessInfo{}, httpErr(http.StatusUnauthorized, err)
	}
	ai := accessInfo{
		user:              data.User,
		service:           data.IsService,
		protectedPrefixes: protectedPrefixes,
		bitViewPrefix:     map[string]bool{},
		bitEditPrefix:     map[string]bool{},
		bitViewMetric:     map[string]bool{},
		bitEditMetric:     map[string]bool{},
		insecureMode:      insecureMode,
	}

	bits := data.Bits
	for b := range bits {
		switch {
		case b == "admin":
			ai.bitAdmin = true
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
		}
	}

	return ai, nil
}

func (ai *accessInfo) protectedMetric(name string) bool {
	for _, p := range ai.protectedPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func (ai *accessInfo) CanViewMetricName(name string) bool {
	if name == format.BuiltinMetricNameBadges && ai.skipBadgesValidation {
		return true
	}
	if ai.insecureMode {
		return true
	}

	return ai.bitViewMetric[name] ||
		hasPrefixAccess(ai.bitViewPrefix, name) ||
		(ai.bitViewDefault && !ai.protectedMetric(name))
}

func (ai *accessInfo) CanViewMetric(metric format.MetricMetaValue) bool {
	return ai.CanViewMetricName(metric.Name)
}

func (ai *accessInfo) canChangeMetricByName(create bool, old format.MetricMetaValue, new_ format.MetricMetaValue) bool {
	if ai.insecureMode || ai.bitAdmin {
		return true
	}

	if !create {
		oldGroup := old.Group
		newGroup := new_.Group
		if oldGroup != nil && newGroup != nil {
			if oldGroup.ID != newGroup.ID {
				return false
			}
		} else if oldGroup != newGroup {
			return false
		}
	}
	oldName := old.Name
	newName := new_.Name

	// we expect that oldName and newName both are in the same group
	return ai.bitEditMetric[oldName] && ai.bitEditMetric[newName] ||
		(hasPrefixAccess(ai.bitEditPrefix, oldName) && hasPrefixAccess(ai.bitEditPrefix, newName)) ||
		(ai.bitEditDefault && !ai.protectedMetric(oldName) && !ai.protectedMetric(newName))
}

func (ai *accessInfo) CanEditMetric(create bool, old format.MetricMetaValue, new_ format.MetricMetaValue) bool {
	if ai.insecureMode {
		return true
	}
	if ai.canChangeMetricByName(create, old, new_) {
		if ai.bitAdmin {
			return true
		}
		if old.Weight != new_.Weight && !(old.Weight == 0 && new_.Weight == 1) {
			return false
		}
		if preKey(old) != preKey(new_) {
			return false
		}
		if preKeyOnly(old) != preKeyOnly(new_) {
			return false
		}
		if skips(old) != skips(new_) {
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

func preKey(m format.MetricMetaValue) uint32 {
	return m.PreKeyFrom
}

func preKeyOnly(m format.MetricMetaValue) bool {
	return m.PreKeyOnly
}

func skips(m format.MetricMetaValue) [3]bool {
	return [3]bool{m.SkipMaxHost, m.SkipMinHost, m.SkipSumSquare}
}

func hasPrefixAccess(m map[string]bool, metric string) bool {
	for prefix := range m {
		if strings.HasPrefix(metric, prefix) {
			return true
		}
	}
	return false
}
