package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouseApi"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"pgregory.net/rand"
)

func main() {
	net := flag.String("statshouse-net", "tcp4", "")
	addr := flag.String("statshouse-addr", "127.0.0.1:13347", "")
	metric := flag.String("metric", "__heartbeat_version", "")
	flag.Parse()

	client := rpc.NewClient(rpc.ClientWithLogf(log.Printf))
	apiClient := tlstatshouseApi.Client{
		Client:  client,
		Network: *net,
		Address: *addr,
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				to := time.Now().Unix()
				from := time.Now().Add(-time.Minute * time.Duration(5+rand.Intn(60*24*2))).Unix()
				qp := tlstatshouseApi.GetQueryPoint{
					AccessToken: "",
					Query: tlstatshouseApi.QueryPoint{
						Version:    2,
						TopN:       1,
						MetricName: *metric,
						TimeFrom:   from,
						TimeTo:     to,
						Function:   tlstatshouseApi.FnCount(),
						TimeShift:  []int64{3600},
					},
				}
				resp := tlstatshouseApi.GetQueryPointResponse{}
				ctx, c := context.WithTimeout(context.Background(), time.Second)
				err := apiClient.GetQueryPoint(ctx, qp, nil, &resp)
				c()
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()
	//fmt.Println(string(resp.WriteJSON(nil)))
}
