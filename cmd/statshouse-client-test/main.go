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

func main() {
	var args argv
	flag.IntVar(&args.n, "n", 100, "number of iterations")
	flag.IntVar(&args.m, "m", 300, "number of series to send per iteration")
	flag.IntVar(&args.maxStrLen, "max-str-len", 32, "maximum string length")
	flag.BoolVar(&args.viewCode, "view-code", false, "open generated source files in Visual Studio Code")
	flag.BoolVar(&args.keepTemp, "keep-temp", false, "do not remove generated temporary files")
	flag.Parse()
	os.Exit(run(args))
}

func run(args argv) int {
	actualC := make(chan series)
	cancel, err := listenUDP(args, actualC)
	if err != nil {
		log.Fatal(err)
	}
	defer cancel()
	data := newTestData(args)
	expected := data.toSeries(args)
	test := func(l lib) int {
		fmt.Printf("*** %q test ***\n", typeName(l))
		if err = runClient(l, data); err != nil {
			log.Println(err)
			return 1 // error
		}
		select {
		case actual := <-actualC:
			if diff := compareSeries(expected, actual); !diff.empty() {
				log.Println(typeName(l), diff.String())
				log.Println("*** FAILED ***")
			} else {
				log.Println("*** PASSED ***")
			}
		case <-time.After(100 * time.Millisecond):
			log.Println("TIMEOUT")
			log.Println("*** FAILED ***")
		}
		return 0 // success
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
	var exitCode int
	for _, l := range targets {
		exitCode += test(l)
	}
	return exitCode
}
