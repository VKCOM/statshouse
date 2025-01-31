// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouseApi"
	"github.com/vkcom/statshouse/internal/format"
)

const (
	// not valid characters in tag names
	queryFilterInSep    = "-"
	queryFilterNotInSep = "~"
)

func ParseTLFunc(str string) (tlstatshouseApi.Function, bool) {
	var res tlstatshouseApi.Function
	switch str {
	case format.ParamQueryFnCount:
		res = tlstatshouseApi.FnCount()
	case format.ParamQueryFnCountNorm:
		res = tlstatshouseApi.FnCountNorm()
	case format.ParamQueryFnCumulCount:
		res = tlstatshouseApi.FnCumulCount()
	case format.ParamQueryFnMin:
		res = tlstatshouseApi.FnMin()
	case format.ParamQueryFnMax:
		res = tlstatshouseApi.FnMax()
	case format.ParamQueryFnAvg:
		res = tlstatshouseApi.FnAvg()
	case format.ParamQueryFnCumulAvg:
		res = tlstatshouseApi.FnCumulAvg()
	case format.ParamQueryFnSum:
		res = tlstatshouseApi.FnSum()
	case format.ParamQueryFnSumNorm:
		res = tlstatshouseApi.FnSumNorm()
	case format.ParamQueryFnStddev:
		res = tlstatshouseApi.FnStddev()
	case format.ParamQueryFnP0_1:
		res = tlstatshouseApi.FnP01()
	case format.ParamQueryFnP1:
		res = tlstatshouseApi.FnP1()
	case format.ParamQueryFnP5:
		res = tlstatshouseApi.FnP5()
	case format.ParamQueryFnP10:
		res = tlstatshouseApi.FnP10()
	case format.ParamQueryFnP25:
		res = tlstatshouseApi.FnP25()
	case format.ParamQueryFnP50:
		res = tlstatshouseApi.FnP50()
	case format.ParamQueryFnP75:
		res = tlstatshouseApi.FnP75()
	case format.ParamQueryFnP90:
		res = tlstatshouseApi.FnP90()
	case format.ParamQueryFnP95:
		res = tlstatshouseApi.FnP95()
	case format.ParamQueryFnP99:
		res = tlstatshouseApi.FnP99()
	case format.ParamQueryFnP999:
		res = tlstatshouseApi.FnP999()
	case format.ParamQueryFnUnique:
		res = tlstatshouseApi.FnUnique()
	case format.ParamQueryFnUniqueNorm:
		res = tlstatshouseApi.FnUniqueNorm()
	case format.ParamQueryFnMaxHost:
		res = tlstatshouseApi.FnMaxHost()
	case format.ParamQueryFnMaxCountHost:
		res = tlstatshouseApi.FnMaxCountHost()
	case format.ParamQueryFnCumulSum:
		res = tlstatshouseApi.FnCumulSum()
	case format.ParamQueryFnDerivativeCount:
		res = tlstatshouseApi.FnDerivativeCount()
	case format.ParamQueryFnDerivativeCountNorm:
		res = tlstatshouseApi.FnDerivativeCountNorm()
	case format.ParamQueryFnDerivativeSum:
		res = tlstatshouseApi.FnDerivativeSum()
	case format.ParamQueryFnDerivativeSumNorm:
		res = tlstatshouseApi.FnDerivativeSumNorm()
	case format.ParamQueryFnDerivativeMin:
		res = tlstatshouseApi.FnDerivativeMin()
	case format.ParamQueryFnDerivativeMax:
		res = tlstatshouseApi.FnDerivativeMax()
	case format.ParamQueryFnDerivativeAvg:
		res = tlstatshouseApi.FnDerivativeAvg()
	case format.ParamQueryFnDerivativeUnique:
		res = tlstatshouseApi.FnDerivativeUnique()
	case format.ParamQueryFnDerivativeUniqueNorm:
		res = tlstatshouseApi.FnDerivativeUniqueNorm()
	default:
		return tlstatshouseApi.Function{}, false
	}
	return res, true
}

func parseFromRows(fromRows string) (RowMarker, error) {
	res := RowMarker{}
	if fromRows == "" {
		return res, nil
	}
	var buf []byte
	if len(buf) < len(fromRows) {
		buf = make([]byte, len(fromRows))
	}
	n, err := base64.RawURLEncoding.Decode(buf, []byte(fromRows))
	if err != nil {
		return res, err
	}
	err = json.Unmarshal(buf[:n], &res)
	if err != nil {
		return res, err
	}
	return res, nil
}

func encodeFromRows(row *RowMarker) (string, error) {
	jsonBytes, err := json.Marshal(row)
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(jsonBytes), nil
}

func validateQuery(metricMeta *format.MetricMetaValue, version string) error {
	if _, ok := format.BuiltinMetrics[metricMeta.MetricID]; ok && version == Version1 {
		return httpErr(http.StatusBadRequest, fmt.Errorf("can't use builtin metric %q with version %q", metricMeta.Name, version))
	}
	return nil
}

func parseQueryFilter(filter []string) (map[string][]string, map[string][]string, error) {
	filterIn := map[string][]string{}
	filterNotIn := map[string][]string{}

	for _, f := range filter {
		inIx := strings.Index(f, queryFilterInSep)
		notInIx := strings.Index(f, queryFilterNotInSep)
		switch {
		case inIx == -1 && notInIx == -1:
			return nil, nil, httpErr(http.StatusBadRequest, fmt.Errorf("invalid %q value: %q", ParamQueryFilter, f))
		case inIx != -1 && (notInIx == -1 || inIx < notInIx):
			ks := f[:inIx]
			k, err := parseTagID(ks)
			if err != nil {
				return nil, nil, err
			}
			v := f[inIx+1:]
			if !format.ValidTagValueForAPI(v) {
				return nil, nil, httpErr(http.StatusBadRequest, fmt.Errorf("invalid %q filter: %q", k, v))
			}
			filterIn[k] = append(filterIn[k], v)
		default:
			ks := f[:notInIx]
			k, err := parseTagID(ks)
			if err != nil {
				return nil, nil, err
			}
			v := f[notInIx+1:]
			if !format.ValidTagValueForAPI(v) {
				return nil, nil, httpErr(http.StatusBadRequest, fmt.Errorf("invalid %q not-filter: %q", k, v))
			}
			filterNotIn[k] = append(filterNotIn[k], v)
		}
	}

	return filterIn, filterNotIn, nil
}
