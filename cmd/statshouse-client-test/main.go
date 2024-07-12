package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

type argv struct {
	n, m      int
	maxStrLen int
	viewCode  bool
	keepTemp  bool
}

func parseCommandLine() (res argv) {
	var flags = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flags.IntVar(&res.n, "n", 100, "number of iterations")
	flags.IntVar(&res.m, "m", 300, "number of series to send per iteration")
	flags.IntVar(&res.maxStrLen, "max-str-len", 32, "maximum string length")
	flags.BoolVar(&res.viewCode, "view-code", false, "open generated source files in Visual Studio Code")
	flags.BoolVar(&res.keepTemp, "keep-temp", false, "do not remove generated temporary files")
	flags.Parse(os.Args[1:])
	return res
}

func main() {
	args := parseCommandLine()
	actualC := make(chan series)
	cancel, err := listenUDP(args, actualC)
	if err != nil {
		log.Println(err)
		return
	}
	defer cancel()
	data := newTestData(args)
	expected := data.toSeries(args)
	test := func(l lib) {
		fmt.Printf("\n*** %q test ***\n", typeName(l))
		if err = runClient(l, data); err != nil {
			log.Println(err)
			return
		}
		select {
		case actual := <-actualC:
			if diff := compareSeries(expected, actual); !diff.empty() {
				log.Println(typeName(l), diff.String())
				fmt.Println("*** FAILED ***")
			} else {
				fmt.Println("*** PASSED ***")
			}
		case <-time.After(100 * time.Millisecond):
			log.Println("TIMEOUT")
			fmt.Println("*** FAILED ***")
		}
	}
	var targets []lib
	clients := []lib{
		newClient(&cppTransport{}, args),
		newClient(&cppRegistry{}, args),
		newClient(&rust{}, args),
		newClient(&java{}, args),
		newClient(&python{}, args),
		newClient(&php{}, args),
		newClient(&golang{}, args),
	}
	for _, c := range clients {
		if c.foundLocalCopy() {
			targets = append(targets, c)
		}
	}
	if len(targets) == 0 {
		// no libraries found locally, download and run all
		targets = clients
	}
	for _, l := range targets {
		test(l)
	}
}
