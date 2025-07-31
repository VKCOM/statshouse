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
	HttpClient *http.Client
	Addr       string
	User       string
	Password   string
	Query      string
	Format     string // "RowBinary" or empty
	Body       []byte // for INSERT operations
	UrlParams  map[string]string
}

// Execute performs the HTTP request to ClickHouse
func (r *ClickHouseHttpRequest) Execute() ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build query
	query := r.Query
	if r.Format != "" {
		query += " FORMAT " + r.Format
	}

	// Build URL
	baseURL := fmt.Sprintf("http://%s/", r.Addr)
	var URL string
	if len(r.UrlParams) > 0 {
		// For INSERT operations with URL parameters
		queryPrefix := url.PathEscape(query)
		URL = fmt.Sprintf("%s?input_format_values_interpret_expressions=0&query=%s", baseURL, queryPrefix)
	} else {
		URL = baseURL
	}

	// Prepare request body
	var body io.Reader
	if r.Body != nil {
		body = bytes.NewReader(r.Body)
	} else {
		body = bytes.NewReader([]byte(query))
	}

	req, err := http.NewRequestWithContext(ctx, "POST", URL, body)
	if err != nil {
		return nil, err
	}
	if r.User != "" {
		req.Header.Set("X-ClickHouse-User", r.User)
	}
	if r.Password != "" {
		req.Header.Set("X-ClickHouse-Key", r.Password)
	}

	resp, err := r.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ClickHouse returned status %d: %s", resp.StatusCode, string(body))
	}

	// Read response body if needed
	if r.Format == "RowBinary" {
		return io.ReadAll(resp.Body)
	}
	return nil, nil
}
