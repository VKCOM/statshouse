package main

import (
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type argv struct {
	n, m      int
	maxStrLen int
	viewCode  bool
	keepTemp  bool
	zeroTime  bool
}

func main() {
	var args argv
	flag.IntVar(&args.n, "n", 100, "number of iterations")
	flag.IntVar(&args.m, "m", 300, "number of series to send per iteration")
	flag.IntVar(&args.maxStrLen, "max-str-len", 32, "maximum string length")
	flag.BoolVar(&args.viewCode, "view-code", false, "open generated source files in Visual Studio Code")
	flag.BoolVar(&args.keepTemp, "keep-temp", false, "do not remove generated temporary files")
	flag.BoolVar(&args.zeroTime, "zero-time", false, "do not compare timestamps")
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
	fail := func(v ...any) int {
		log.Println(v...)
		log.Println("*** FAILED ***")
		return 1
	}
	test := func(lib lib) int {
		matches, err := filepath.Glob("test_template*.txt")
		if err != nil {
			log.Fatal(err)
		}
		if len(matches) == 0 {
			log.Fatal("test template not found")
		}
		var failCount int
		for _, path := range matches {
			log.Printf("*** %s ***\n", path)
			file, err := os.Open(path)
			if err != nil {
				log.Fatal(err)
			}
			var sb strings.Builder
			if _, err = io.Copy(&sb, file); err != nil {
				log.Fatal(err)
			}
			if err = runClient(lib, sb.String(), data); err != nil {
				failCount += fail(err)
				continue
			}
			select {
			case actual := <-actualC:
				if diff := compareSeries(expected, actual); !diff.empty() {
					failCount += fail(diff.String())
				} else {
					log.Println("*** PASSED ***")
				}
			case <-time.After(100 * time.Millisecond):
				failCount += fail("TIMEOUT")
			}
		}
		return failCount
	}
	libs := []lib{
		&cpp{},
		&rust{},
		&java{},
		&python{},
		&php{},
		&golang{},
	}
	for _, lib := range libs {
		path := search(lib)
		lib.init(lib, args, path)
		if path != "" {
			return test(lib)
		}
	}
	// no library was found in the current directory, download and run them all
	var failCount int
	for _, lib := range libs {
		failCount += test(lib)
	}
	return failCount
}
