package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"slices"
	"sort"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/receiver"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type series map[tags]map[uint32]*value
type tag [2]string // name, value
type tags [17]tag  // metric name + 16 tags

type value struct {
	counter float64
	values  []float64
	uniques []int64
}

type handler struct {
	sink func(*tlstatshouse.MetricBytes)
}

func (h handler) HandleMetrics(args data_model.HandlerArgs) (data_model.MappedMetricHeader, bool) {
	h.sink(args.MetricBytes)
	return data_model.MappedMetricHeader{}, false
}

func (handler) HandleParseError(pkt []byte, err error) {
	log.Fatalln(pkt, err)
}

func listen(args argv, ch chan series) (func(), error) {
	var addr = ":13337"
	var serve func(func(*tlstatshouse.MetricBytes)) error
	var cancel func() error
	switch args.network {
	case "udp":
		listenerUDP, err := receiver.ListenUDP("udp4", addr, 16*1024*1024, false, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		cancel = listenerUDP.Close
		log.Printf("listen UDP %s\n", addr)
		serve = func(f func(*tlstatshouse.MetricBytes)) error {
			return listenerUDP.Serve(handler{f})
		}
	case "tcp":
		listenerTCP, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		log.Printf("listen TCP %s\n", addr)
		hijackL := rpc.NewHijackListener(listenerTCP.Addr())
		hijackH := func(conn *rpc.HijackConnection) {
			conn.Magic = conn.Magic[len(receiver.TCPPrefix):]
			hijackL.AddConnection(conn)
		}
		server := rpc.NewServer(rpc.ServerWithSocketHijackHandler(hijackH))
		cancel = server.Close
		serve = func(f func(*tlstatshouse.MetricBytes)) error {
			go receiver.NewTCPReceiver(nil, nil).Serve(handler{f}, hijackL)
			return server.Serve(listenerTCP)
		}
	default:
		return nil, fmt.Errorf("not supported transport %s", args.network)
	}
	syncCh := make(chan int, 1)
	go func() {
		defer close(syncCh)
		res := make(series, args.m)
		var n int
		syncCh <- 1 // started
		if err := serve(func(b *tlstatshouse.MetricBytes) {
			if bytes.Equal(b.Name, endOfIterationMarkBytes) {
				n += int(b.Counter)
				log.Println("iteration #", n)
				if n == args.n {
					if args.zeroTime {
						res.sort()
					}
					ch <- res
					res = make(series, len(res))
					n = 0
				}
			} else {
				res.addMetricBytes(b)
			}
		}); err != nil {
			log.Fatalln(err)
		}
	}()
	<-syncCh // wait started
	return func() {
		cancel()
		<-syncCh // wait closed
	}, nil
}

func (s series) addMetricBytes(b *tlstatshouse.MetricBytes) {
	tags := tags{{"", string(b.Name)}}
	for i := 0; i < len(b.Tags); i++ {
		if len(b.Tags[i].Value) != 0 {
			tags[i+1] = tag{string(b.Tags[i].Key), string(b.Tags[i].Value)}
		}
	}
	sort.Slice(tags[:], func(i, j int) bool {
		return slices.Compare(tags[i][:], tags[j][:]) < 0
	})
	vals := s[tags]
	if vals == nil {
		vals = map[uint32]*value{}
		s[tags] = vals
	}
	val := vals[b.Ts]
	if val == nil {
		val = &value{}
		vals[b.Ts] = val
	}
	val.addMetricBytes(b)
}

func (s series) sort() {
	for _, sec := range s {
		for _, val := range sec {
			val.sort()
		}
	}
}

func (v *value) addMetricBytes(b *tlstatshouse.MetricBytes) {
	v.counter += b.Counter
	v.values = append(v.values, b.Value...)
	v.uniques = append(v.uniques, b.Unique...)
}

func (v *value) addMetric(b metric) {
	v.counter += b.Count
	v.values = append(v.values, b.Values...)
	v.uniques = append(v.uniques, b.Uniques...)
}

func (v *value) sort() {
	slices.Sort(v.values)
	slices.Sort(v.uniques)
}
