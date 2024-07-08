package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"pgregory.net/rand"
)

// primitive tool for testing conveyor jitter
// alternative agent port, because default agent receives tons of other metrics from API
func main() {
	conn, err := net.Dial("udp", "127.0.0.1:13334")
	if err != nil {
		log.Fatal("Dial error: %v", err)
	}

	ts := int64(0)
	for {
		time.Sleep(time.Millisecond * 100)
		ts2 := time.Now().Unix()
		if ts2 == ts {
			continue
		}
		ts = ts2
		str := fmt.Sprintf(`{"metrics":[{"ts":%d, "name":"gbuteyko_investigation","tags":{"1":"I_test_statshouse","2":"2"},"counter":1}]}`, ts)
		fmt.Println(str)
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(700))) // simulate slow packet delivery
		if _, err := conn.Write([]byte(str)); err != nil {
			log.Printf("error writing UDP: %v", err)
		}
	}
}
