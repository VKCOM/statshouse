// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"

	"github.com/VKCOM/statshouse/internal/pcache"
)

type tagValueInverseLoader struct {
	metaClient  *tlmetadata.Client
	loadTimeout time.Duration
}

func (l tagValueInverseLoader) load(ctx context.Context, tagValueID string, _ interface{}) (pcache.Value, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, l.loadTimeout)
	defer cancel()
	parsed, err := strconv.ParseInt(tagValueID, 10, 32)
	if err != nil {
		return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("%v not a integer", tagValueID))
	}
	resp := tlmetadata.GetInvertMappingResponse{}
	err = l.metaClient.GetInvertMapping(ctx, tlmetadata.GetInvertMapping{
		Id: int32(parsed),
	}, nil, &resp)
	if err != nil {
		return nil, 0, err
	}
	if resp.IsKeyNotExists() {
		return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("tag value for %v not found", tagValueID))
	}
	if response, ok := resp.AsGetInvertMappingResponse(); ok {
		return pcache.StringToValue(response.Key), 0, nil
	}
	return nil, 0, fmt.Errorf("can't parse response: %d", resp.TLTag())
}

type tagValueLoader struct {
	loadTimeout time.Duration
	metaClient  *tlmetadata.Client
}

func (l tagValueLoader) load(ctx context.Context, tagValue string, _ interface{}) (pcache.Value, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, l.loadTimeout)
	defer cancel()
	req := tlmetadata.GetMapping{
		Key: tagValue,
	}
	req.SetCreateIfAbsent(false) // We do not need to validate tagValue, because we never ask to create here
	resp := tlmetadata.GetMappingResponse{}
	err := l.metaClient.GetMapping(ctx, req, nil, &resp)
	if err != nil {
		return nil, 0, err
	}
	if resp.IsKeyNotExists() {
		return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("tag value ID for %q not found", tagValue))
	}
	if response, ok := resp.AsGetMappingResponse(); ok {
		return pcache.Int32ToValue(response.Id), 0, nil
	}
	return nil, 0, fmt.Errorf("can't parse response: %d", resp.TLTag())
}
