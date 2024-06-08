package main

import (
	"bytes"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

const (
	randomWalkPrefix = "random_walk"
	randomWalkTotal  = 6
)

// http://localhost:10888/api/dashboard
const dashboardTempl = `
{
  "dashboard":{
    "name":"random walk",
    "description":"",
    "version":0,
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
      "tagSync":[
        
      ],
      "plots":[
	  {{range .}}
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
        },
	  {{end}}
      ],
      "timeShifts":[ ],
      "tabNum":-1,
      "variables":[ ],
      "searchParams":[ ]
    }
  }
}
`

type plot struct {
	Id   int
	Name string
}

var (
	i int64
	p = message.NewPrinter(language.English)
)

func printCounter() {
	log.Printf("#%s", p.Sprintf("%d", i))
}

func main() {
	var (
		n            int64
		totalMetrics int
		done         = make(chan os.Signal, 1)
	)
	flag.IntVar(&totalMetrics, "m", 6, "number of metrics")
	flag.Parse()
	// log.SetPrefix(fmt.Sprintf("%d %q ", os.Getpid(), totalMetrics))
	// signal.Notify(done, os.Interrupt, syscall.SIGTERM)
	// printCounter()
	// defer printCounter()

	plots := make([]plot, totalMetrics)
	for i, _ := range plots {
		plots[i].Id = i
		plots[i].Name = fmt.Sprint(randomWalkPrefix, i)
	}
	dash := template.New("random_walk_dashboard")
	dash.Parse(dashboardTempl)
	buf := new(bytes.Buffer)
	err := dash.Execute(buf, plots)

	if err != nil {
		panic(err)
	}
	http.NewRequest(http.MethodPut, "http://localhost:10888/api/dashboard", buf)

	ticker := time.NewTicker(time.Second)
	// values := make([]float64, totalMetrics)
	// rng := rand.New()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			// 		for m := 0; m < totalMetrics; m++ {
			// 			statshouse.MetricNamed(fmt.Sprint(randomWalkPrefix, m), statshouse.NamedTags{}).Value(values[m])
			// 			sign := float64(1)
			// 			if rng.Int31n(2) == 1 {
			// 				sign = float64(-1)
			// 			}
			// 			values[m] += rng.Float64() * sign
			// 		}
		}
		n++
	}
	<-done
}
