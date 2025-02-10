package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/mailru/easyjson"
)

type Client struct {
	baseURL    string
	userToken  string
	httpClient *http.Client
}

func NewClient(baseURL, userToken string) *Client {
	return &Client{
		baseURL:    baseURL,
		userToken:  userToken,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) BaseURL() string {
	return c.baseURL
}

func (c *Client) doRequest(ctx context.Context, method, path string, params url.Values, body io.Reader, result easyjson.Unmarshaler) error {
	reqURL := c.baseURL + path
	if params != nil {
		reqURL += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(userTokenName, c.userToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	respBox := &Response{
		Data: result,
	}
	respBody, _ := io.ReadAll(resp.Body)
	err = easyjson.Unmarshal(respBody, respBox)
	if resp.StatusCode >= http.StatusBadRequest {
		e := respBox.Error
		if err != nil {
			e = string(respBody)
		}
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, e)
	}
	if len(respBox.Error) > 0 {
		return fmt.Errorf("service error: %s", respBox.Error)
	}
	return nil
}

func (c *Client) GetMetricsList(ctx context.Context) ([]string, error) {
	var resp GetMetricsListResp

	err := c.doRequest(ctx, "GET", "/api/metrics-list", nil, nil, &resp)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(resp.Metrics))
	for _, m := range resp.Metrics {
		names = append(names, m.Name)
	}
	return names, nil
}

func (c *Client) GetMetric(ctx context.Context, name string) (*MetricInfo, error) {
	query := url.Values{}
	query.Set("s", name)

	var resp MetricInfo
	err := c.doRequest(ctx, "GET", "/api/metric", query, nil, &resp)
	return &resp, err
}

func (c *Client) PostMetric(ctx context.Context, metric *MetricInfo) error {
	query := url.Values{}
	query.Set("s", metric.Metric.Name)

	body, err := easyjson.Marshal(metric)
	if err != nil {
		return err
	}
	err = c.doRequest(ctx, "POST", "/api/metric", query, bytes.NewReader(body), nil)
	return err
}

func (c *Client) ListDashboards(ctx context.Context) ([]dashboardShortInfo, error) {
	var resp GetDashboardListResp
	err := c.doRequest(ctx, "GET", "/api/dashboards-list", nil, nil, &resp)
	return resp.Dashboards, err
}

func (c *Client) GetDashboard(ctx context.Context, id int32) (*DashboardInfo, error) {
	query := url.Values{"id": {fmt.Sprint(id)}}
	var resp DashboardInfo
	err := c.doRequest(ctx, "GET", "/api/dashboard", query, nil, &resp)
	return &resp, err
}

func (c *Client) PostDashboard(ctx context.Context, dashboard *DashboardInfo) error {
	body, err := json.Marshal(dashboard)
	if err != nil {
		return err
	}
	err = c.doRequest(ctx, "POST", "/api/dashboard", nil, bytes.NewReader(body), nil)
	return err
}

func (c *Client) PutDashboard(ctx context.Context, dashboard *DashboardInfo) error {
	body, err := json.Marshal(dashboard)
	if err != nil {
		return err
	}
	err = c.doRequest(ctx, "PUT", "/api/dashboard", nil, bytes.NewReader(body), nil)
	return err
}
