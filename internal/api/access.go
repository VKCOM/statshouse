// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/vkuth"
)

type accessInfo struct {
	user              string
	service           bool
	protectedPrefixes []string
	bitAdmin          bool
	bitDeveloper      bool
	bitViewDefault    bool
	bitEditDefault    bool
	bitViewPrefix     map[string]bool
	bitEditPrefix     map[string]bool
	bitViewMetric     map[string]bool
	bitEditMetric     map[string]bool
}

func parseAccessToken(jwtHelper *vkuth.JWTHelper,
	accessToken string,
	protectedPrefixes []string,
	localMode bool,
	insecureMode bool) (accessInfo, error) {
	if localMode || insecureMode {
		ai := accessInfo{
			user:              "@insecure_mode",
			protectedPrefixes: protectedPrefixes,
			bitViewDefault:    true,
			bitEditDefault:    true,
			bitAdmin:          localMode,
			bitDeveloper:      localMode,
			bitViewPrefix:     map[string]bool{},
			bitEditPrefix:     map[string]bool{},
			bitViewMetric:     map[string]bool{},
			bitEditMetric:     map[string]bool{},
		}
		return ai, nil
	}
	if accessToken == "" { // For better error text. Validation would return cryptic "invalid number of segments"
		return accessInfo{}, httpErr(http.StatusUnauthorized, fmt.Errorf("empty access token, auth service (vkuth) is down"))
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
			ai.bitViewPrefix[extractNamespace(b[len("view_prefix."):])] = true
		case strings.HasPrefix(b, "edit_prefix."):
			ai.bitEditPrefix[extractNamespace(b[len("edit_prefix."):])] = true
		case strings.HasPrefix(b, "view_metric."):
			ai.bitViewMetric[extractNamespace(b[len("view_metric."):])] = true
		case strings.HasPrefix(b, "edit_metric."):
			ai.bitEditMetric[extractNamespace(b[len("edit_metric."):])] = true
		// use another bit, because vkuth token can't contain ':'
		case strings.HasPrefix(b, "view_namespace."):
			ai.bitViewPrefix[b[len("view_namespace."):]+":"] = true
		case strings.HasPrefix(b, "edit_namespace."):
			ai.bitEditPrefix[b[len("edit_namespace."):]+":"] = true
		}
	}

	return ai, nil
}

// ':' is reserved by vkuth. We use '@' as namespace separator in bits
func extractNamespace(bit string) string {
	return strings.Replace(bit, "@", ":", 1)
}

func (ai *accessInfo) toMetadata() string {
	m := metadata{
		UserEmail: ai.user,
		UserName:  "",
		UserRef:   "",
	}
	res, _ := json.Marshal(&m)
	return string(res)
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
	if format.RemoteConfigMetric(name) && !ai.bitAdmin {
		return false // remote config can only be viewed by administrators
	}
	return ai.bitViewMetric[name] ||
		hasPrefixAccess(ai.bitViewPrefix, name) ||
		(ai.bitViewDefault && !ai.protectedMetric(name))
}

func (ai *accessInfo) CanViewMetric(metric format.MetricMetaValue) bool {
	return ai.CanViewMetricName(metric.Name)
}

func (ai *accessInfo) canChangeMetricByName(create bool, old format.MetricMetaValue, new_ format.MetricMetaValue) bool {
	if ai.bitAdmin {
		return true
	}

	oldName := old.Name
	newName := new_.Name

	if format.RemoteConfigMetric(oldName) || format.RemoteConfigMetric(newName) {
		return false // remote config can only be set by administrators
	}
	return ai.bitEditMetric[oldName] && ai.bitEditMetric[newName] ||
		(hasPrefixAccess(ai.bitEditPrefix, oldName) && hasPrefixAccess(ai.bitEditPrefix, newName)) ||
		(ai.bitEditDefault && !ai.protectedMetric(oldName) && !ai.protectedMetric(newName))
}

func (ai *accessInfo) CanEditMetric(create bool, old format.MetricMetaValue, new_ format.MetricMetaValue) bool {
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
