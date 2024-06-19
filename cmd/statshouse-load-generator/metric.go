package main

import (
	"bytes"
	"context"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
)

const metricTempl = `{
  "metric":{
    "description":"",
    "kind":"mixed",
    "name":"{{.Name}}",
    "metric_id":{{.MetricId}},
	{{if ge .Version 0 }}
    "version":{{.Version}},
	{{end}}
    "string_top_name":"",
    "string_top_description":"",
    "weight":1,
    "resolution":1,
    "visible":true,
    "tags":[
      {
        "name":"",
        "description":"environment",
        "raw":false
      },
      {
        "name":"client",
        "description":"client id - raw tag",
        "raw":true
      },
      {
        "name":"random",
        "description":"random sting used to test mapping flood",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      },
      {
        "name":"",
        "description":"",
        "raw":false
      }
    ],
    "tags_draft":{ },
    "pre_key_from":0,
    "skip_max_host":false,
    "skip_min_host":false,
    "skip_sum_square":false,
    "pre_key_only":false
  }
}`

type MetricData struct {
	Name string `json:"name"`
}

type MetricList struct {
	Metrics []MetricData `json:"metrics"`
}

type MetricListResp struct {
	Data MetricList `json:"data"`
}

type MetricInfo struct {
	Name     string `json:"name"`
	MetricId int    `json:"metric_id"`
	Version  int    `json:"version"`
}

type MetricInfoBox struct {
	Metric MetricInfo `json:"metric"`
}

type MetricInfoResp struct {
	Data MetricInfoBox `json:"data"`
}

func ensureMetrics(ctx context.Context, client http.Client, names []string) {
	resp, err := client.Get("http://localhost:10888/api/metrics-list")
	if err != nil {
		panic(err)
	}
	buffer := new(bytes.Buffer)
	buffer.ReadFrom(resp.Body)
	var metricListResp MetricListResp
	err = json.Unmarshal(buffer.Bytes(), &metricListResp)
	if err != nil {
		panic(err)
	}
	buffer.Reset()
	allMetrics := make(map[string]bool)
	for _, metricData := range metricListResp.Data.Metrics {
		allMetrics[metricData.Name] = true
	}

	newMetrics := make([]string, 0)
	existingMetrics := make([]string, 0)

	for _, name := range names {
		if _, exists := allMetrics[name]; exists {
			existingMetrics = append(existingMetrics, name)
		} else {
			newMetrics = append(newMetrics, name)
		}
	}

	for _, name := range newMetrics {
		select {
		case <-ctx.Done():
			return
		default:
		}
		log.Println("creating metric:", name)
		metric := template.New("metric template")
		metric.Parse(metricTempl)
		err := metric.Execute(buffer, MetricInfo{
			Name: name,
		})
		if err != nil {
			panic(err)
		}
		req, err := http.NewRequest(http.MethodPost, "http://localhost:10888/api/metric", buffer)
		if err != nil {
			panic(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		buffer.Reset()
		if resp.StatusCode != 200 {
			log.Fatalln("got", resp.Status, "in request", resp.Request.Method, resp.Request.URL)
		}
	}

	for _, name := range existingMetrics {
		select {
		case <-ctx.Done():
			return
		default:
		}
		log.Println("updating metric:", name)
		resp, err = client.Get("http://localhost:10888/api/metric?s=" + name)
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != 200 {
			log.Fatalln("got", resp.Status, "in request", resp.Request.Method, resp.Request.URL)
		}

		buffer.ReadFrom(resp.Body)
		var metricInfoResp MetricInfoResp
		err = json.Unmarshal(buffer.Bytes(), &metricInfoResp)
		if err != nil {
			panic(err)
		}
		buffer.Reset()
		metricInfo := metricInfoResp.Data.Metric

		metric := template.New("metric template")
		metric.Parse(metricTempl)
		err = metric.Execute(buffer, metricInfo)
		if err != nil {
			panic(err)
		}
		req, err := http.NewRequest(http.MethodPost, "http://localhost:10888/api/metric", buffer)
		if err != nil {
			panic(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		buffer.Reset()
		if resp.StatusCode != 200 {
			log.Fatalln("got", resp.Status, "in request", resp.Request.Method, resp.Request.URL)
		}
	}
}
