package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"pgregory.net/rand"
)

var endOfIterationMark = "end_of_iteration"
var endOfIterationMarkBytes []byte = []byte(endOfIterationMark)

type testData struct {
	NumberOfIterations int
	Metrics            []metric
}

type metric struct {
	Timestamp uint32
	Name      string
	Tags      [][2]string
	Kind      int       // 0 counter, 1 value, 2 unique
	Count     float64   // always set
	Values    []float64 // Kind == 1
	Uniques   []int64   // Kind == 2
}

func newTestData(args argv) testData {
	log.Printf("generate random data of length %d", args.m)
	str := make([]byte, args.maxStrLen)
	values := make([]float64, args.maxStrLen)
	uniques := make([]int64, args.maxStrLen)
	r := rand.New()
	for i := range str {
		str[i] = 'U'
		values[i] = r.Float64() * 100
		uniques[i] = r.Int63()
	}
	now := uint32(time.Now().Unix())
	res := make([]metric, args.m+1)
	for i := 0; i < args.m; i++ {
		m := &res[i]
		if !args.zeroTime {
			m.Timestamp = r.Uint32n(now)
		}
		m.Name = string(str[:rand.Intn(len(str)-1)+1])
		m.Tags = make([][2]string, r.Intn(args.tagsNum))
		for i, j := r.Intn(args.tagsNum), 0; j < len(m.Tags); i, j = i+1, j+1 {
			m.Tags[j] = [2]string{strconv.Itoa(i % args.tagsNum), string(str[:rand.Intn(len(str)-1)+1])}
		}
		m.Kind = r.Intn(3)
		switch m.Kind {
		case 0: // counter
			m.Count = 1
		case 1: // values
			m.Values = values[:r.Intn(len(values)-1)+1]
		case 2: // uniques
			m.Uniques = uniques[:r.Intn(len(uniques)-1)+1]
		}
	}
	res[args.m].Name = endOfIterationMark
	res[args.m].Count = 1
	return testData{args.n, res}
}

func (t testData) Include(url string) string {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	path := path.Base(req.URL.Path)
	if _, err := os.Stat(path); err == nil {
		return fmt.Sprintf("#include %q", path)
	}
	log.Printf("download %s\n", url)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	if _, err = io.Copy(&sb, resp.Body); err != nil {
		panic(err)
	}
	return sb.String()
}

func (t testData) toSeries(args argv) series {
	res := make(series)
	for _, m := range t.Metrics[:len(t.Metrics)-1] { // exclude EOF metric
		tags := tags{{"", string(m.Name)}}
		for i := 0; i < len(m.Tags); i++ {
			tags[i+1] = m.Tags[i]
		}
		sort.Slice(tags[:], func(i, j int) bool {
			return slices.Compare(tags[i][:], tags[j][:]) < 0
		})
		vals := res[tags]
		if vals == nil {
			vals = map[uint32]*value{}
			res[tags] = vals
		}
		val := vals[m.Timestamp]
		if val == nil {
			val = &value{}
			vals[m.Timestamp] = val
		}
		for i := 0; i < args.n; i++ {
			val.addMetric(m)
		}
	}
	if args.zeroTime {
		res.sort()
	}
	return res
}
