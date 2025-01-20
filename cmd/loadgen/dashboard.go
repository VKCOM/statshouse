package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	metricPrefix        = "random_walk"
	randomWalkDashboard = "Random walk"
	randomWalkTotal     = 6
)

const dashboardTempl = `
{
  "dashboard":{
    "name":"Random walk",
    {{if ge .Version 0 }}
    "dashboard_id": {{.Id}},
    "version": {{.Version}},
    "update_time": {{.NowTs}},
    {{end}}
    "description":"",
    "data":{
      "live":true,
      "dashboard":{
        "name":"random walk",
        "description":"",
        "groupInfo":[]
      },
      "timeRange":{
        "to":0,
        "from":-300
      },
      "eventFrom":0,
      "tagSync":[ ],
      "plots":[
      {{range $index, $element := .Plots }}
      {{if $index}},{{end}}
        {
          "id":"{{.Id}}",
          "metricName":"{{.Name}}",
          "customName":"",
          "customDescription":"",
          "what":[
            "avg"
          ],
          "customAgg":0,
          "groupBy":[
            
          ],
          "filterIn":{
            
          },
          "filterNotIn":{
            
          },
          "numSeries":5,
          "useV2":true,
          "yLock":{
            "min":0,
            "max":0
          },
          "maxHost":false,
          "promQL":"",
          "type":0,
          "events":[ ],
          "eventsBy":[ ],
          "eventsHide":[ ],
          "totalLine":false,
          "filledGraph":true
        }
      {{end}}
      ],
      "timeShifts":[ ],
      "tabNum":-1,
      "variables":[ ],
      "searchParams":[
        {{range $index, $element := .SearchParams }}
        {{if $index}},{{end}}
        ["{{$element.Key}}", "{{$element.Value}}"]
        {{end}}
      ]
    }
  }
}
`

type plot struct {
	Id   int
	Name string
}

type searchParam struct {
	Key   string
	Value string
}

type dashboardListResponse struct {
	Data dashboardList `json:"data"`
}

type dashboardList struct {
	Dashboards []dashboardId `json:"dashboards"`
}

type dashboardId struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

type dashboardMeta struct {
	Version int `json:"version"`
}

type dashboardMetaData struct {
	Meta dashboardMeta `json:"dashboard"`
}

type dashboardMetaResponse struct {
	Data dashboardMetaData `json:"data"`
}

type dashboardTemplData struct {
	Id           int
	Version      int
	Plots        []plot
	NowTs        int64
	SearchParams []searchParam
}

func renderDashboardCreatePayload(dashboardId, dashboardVersion int, plots []plot) *bytes.Buffer {
	dash := template.New(randomWalkDashboard)
	dash.Parse(dashboardTempl)
	buffer := new(bytes.Buffer)

	// Prepare searchParams
	searchParams := []searchParam{
		{"tn", "-2"},
		{"dn", randomWalkDashboard},
		{"t", "0"},
		{"f", "-300"},
	}
	op := make([]string, 0, len(plots))
	for i, plot := range plots {
		searchParams = append(searchParams, searchParam{fmt.Sprintf("t%d.s", i), plot.Name})
		searchParams = append(searchParams, searchParam{fmt.Sprintf("t%d.qw", i), "avg"})
		op = append(op, fmt.Sprintf("%d", i))
	}
	searchParams = append(searchParams, searchParam{"op", strings.Join(op, ".")})
	searchParams = append(searchParams, searchParam{"g0.n", fmt.Sprint(len(plots))})

	err := dash.Execute(buffer, dashboardTemplData{dashboardId, dashboardVersion, plots, time.Now().Unix(), searchParams})
	if err != nil {
		panic(err)
	}
	return buffer
}

func checkResponse(resp *http.Response, err error) {
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != 200 {
		log.Fatalln("got", resp.Status, "in request", resp.Request.Method, resp.Request.URL)
	}
}

func ensureDashboardExists(client http.Client, numberOfMetrics int) {
	resp, err := client.Get("http://localhost:10888/api/dashboards-list")
	checkResponse(resp, err)
	var listResponse dashboardListResponse
	buffer := new(bytes.Buffer)
	_, err = buffer.ReadFrom(resp.Body)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(buffer.Bytes(), &listResponse)
	if err != nil {
		panic(err)
	}
	buffer.Reset()
	dashboardId := -1
	for _, dashboard := range listResponse.Data.Dashboards {
		if dashboard.Name == randomWalkDashboard {
			dashboardId = dashboard.Id
		}
	}
	dashboardVersion := -1
	if dashboardId != -1 {
		log.Printf("found dashboard %s", randomWalkDashboard)
		resp, err = client.Get(fmt.Sprintf("http://localhost:10888/api/dashboard?id=%d", dashboardId))
		checkResponse(resp, err)
		_, err = buffer.ReadFrom(resp.Body)
		if err != nil {
			panic(err)
		}
		var metaResponse dashboardMetaResponse
		err = json.Unmarshal(buffer.Bytes(), &metaResponse)
		if err != nil {
			panic(err)
		}
		dashboardVersion = metaResponse.Data.Meta.Version
		buffer.Reset()
	}

	plots := make([]plot, numberOfMetrics)
	for i := range plots {
		plots[i].Id = i
		plots[i].Name = fmt.Sprint(metricPrefix, i)
	}
	buffer = renderDashboardCreatePayload(dashboardId, dashboardVersion, plots)
	var req *http.Request
	if dashboardVersion < 0 {
		log.Println("creating dashboard:", randomWalkDashboard)
		req, err = http.NewRequest(http.MethodPut, "http://localhost:10888/api/dashboard", buffer)
	} else {
		log.Println("updating dashboard:", randomWalkDashboard)
		req, err = http.NewRequest(http.MethodPost, "http://localhost:10888/api/dashboard", buffer)
	}
	if err != nil {
		panic(err)
	}
	resp, err = client.Do(req)
	checkResponse(resp, err)
}
