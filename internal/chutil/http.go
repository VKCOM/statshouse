// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package chutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// ClickHouseHttpRequest represents a request to ClickHouse HTTP interface
type ClickHouseHttpRequest struct {
	httpClient *http.Client
	addr       string
	user       string
	password   string
	query      string
	format     string // "RowBinary" or empty
	body       []byte // for INSERT operations
	urlParams  map[string]string
}

// Execute performs the HTTP request to ClickHouse
func (r *ClickHouseHttpRequest) Execute() ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build query
	query := r.query
	if r.format != "" {
		query += " FORMAT " + r.format
	}

	// Build URL
	baseURL := fmt.Sprintf("http://%s/", r.addr)
	var URL string
	if len(r.urlParams) > 0 {
		// For INSERT operations with URL parameters
		queryPrefix := url.PathEscape(query)
		URL = fmt.Sprintf("%s?input_format_values_interpret_expressions=0&query=%s", baseURL, queryPrefix)
	} else {
		URL = baseURL
	}

	// Prepare request body
	var body io.Reader
	if r.body != nil {
		body = bytes.NewReader(r.body)
	} else {
		body = bytes.NewReader([]byte(query))
	}

	req, err := http.NewRequestWithContext(ctx, "POST", URL, body)
	if err != nil {
		return nil, err
	}
	if r.user != "" {
		req.Header.Set("X-ClickHouse-User", r.user)
	}
	if r.password != "" {
		req.Header.Set("X-ClickHouse-Key", r.password)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ClickHouse returned status %d: %s", resp.StatusCode, string(body))
	}

	// Read response body if needed
	if r.format == "RowBinary" {
		return io.ReadAll(resp.Body)
	}
	return nil, nil
}
