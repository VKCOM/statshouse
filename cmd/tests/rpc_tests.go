package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouseApi"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
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
	to := time.Now().Unix()
	from := time.Now().Add(-time.Minute * 5).Unix()
	qp := tlstatshouseApi.GetQueryPoint{
		AccessToken: "",
		Query: tlstatshouseApi.QueryPoint{
			Version:    2,
			TopN:       1,
			MetricName: *metric,
			TimeFrom:   from,
			TimeTo:     to,
			Function:   tlstatshouseApi.FnCount(),
		},
	}
	resp := tlstatshouseApi.GetQueryPointResponse{}
	err := apiClient.GetQueryPoint(context.Background(), qp, nil, &resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(resp.WriteJSON(nil)))
}
