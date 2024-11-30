package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type kv struct {
	s string
	i int
}

func isEmpty(t format.MetricMetaTag) bool {
	return t.Name == "" && t.Description == "" && !t.Raw && len(t.ID2Value) == 0 && len(t.ValueComments) == 0 &&
		!t.SkipMapping
}

func main() {
	net := flag.String("metadata-net", "tcp4", "")
	addr := flag.String("metadata-addr", "127.0.0.1:13347", "")
	actor := flag.Int64("metadata-actor-id", 0, "")

	prefix := flag.String("prefix", "__heartbeat_version", "")
	tag := flag.String("tag", "", "")
	mapTO := flag.Int("map-tp", -1, "")
	limit := flag.Int("limit", 10, "")

	flag.Parse()
	client := rpc.NewClient(rpc.ClientWithLogf(log.Printf))
	apiClient := &tlmetadata.Client{
		Client:  client,
		Network: *net,
		Address: *addr,
		ActorID: *actor,
	}
	metadataLoader := metajournal.NewMetricMetaLoader(apiClient, time.Second*60)
	dc, err := pcache.OpenDiskCache("./cache1", time.Second)
	if err != nil {
		panic(err)
	}
	metricStorage := metajournal.MakeMetricsStorage("", dc, nil)
	metricStorage.Journal().Start(nil, nil, metadataLoader.LoadJournal)
	time.Sleep(time.Second * 3)
	ready := map[int]int{}
	readyMetric := map[string]struct{}{}
	stats := map[string]map[string]struct{}{}
	metrics := map[string]*format.MetricMetaValue{}
	for _, m := range metricStorage.MetricsByID {
		if !strings.HasPrefix(m.Name, *prefix) {
			continue
		}
		for s := range m.TagsDraft {
			_, ok := stats[s]
			if !ok {
				stats[s] = map[string]struct{}{}
			}
			ok = true
			for _, metaTag := range m.Tags {
				if metaTag.Name == s {
					ok = false
				}
			}
			if ok {
				if s == *tag && isEmpty(m.Tags[*mapTO]) {
					metrics[m.Name] = m
				}
				stats[s][m.Name] = struct{}{}
			}
		}
		for i, t := range m.Tags {
			if t.Name != *tag {
				continue
			}
			readyMetric[m.Name] = struct{}{}
			ready[i]++
		}
	}
	fmt.Println("Tag distribution")
	c := 0
	for k, v := range ready {
		fmt.Println(k, v)
		c += v
	}
	fmt.Println("Ready metrics", len(readyMetric))
	fmt.Println("Metrics to update", len(metrics))
	//for _, value := range metrics {
	//	fmt.Println(value.Name)
	//}
	c = 0
	for _, m := range metrics {
		if c >= *limit {
			return
		}
		if _, ok := m.TagsDraft[*tag]; !ok {
			panic(" m.TagsDraft[*tag]")
		}
		if m.Tags[*mapTO].Name != "" {
			panic("m.Tags[*mapTO].Name != ''")
		}
		t := m.TagsDraft[m.Name]
		t.Name = *tag
		delete(m.TagsDraft, *tag)
		m.Tags[*mapTO] = t
		fmt.Println("TRY TO MAP", m.Name)
		_, err := metadataLoader.SaveMetric(context.Background(), *m, "auto-mapping")
		if err != nil {
			panic(err)
		}
		c++
	}
	//fmt.Println("ALL", c)
	//kvs := []kv{}
	//for k, v := range stats {
	//	kv := kv{
	//		s: k,
	//		i: len(v),
	//	}
	//	kvs = append(kvs, kv)
	//}
	//sort.Slice(kvs, func(i, j int) bool {
	//	return kvs[i].i > kvs[j].i
	//})
	//for _, k := range kvs {
	//	fmt.Println(k.s, k.i)
	//}

}
