package main

import (
	"flag"
	"log"
	"os"
	"time"
)

type argv struct {
	n, m      int
	maxStrLen int
	viewCode  bool
	keepTemp  bool
	zeroTime  bool
	network   string
}

// Consider increasing UDP receive buffer size up to 16M:
// sysctl -w net.core.rmem_max=16777216
func main() {
	var args argv
	flag.IntVar(&args.n, "n", 100, "number of iterations")
	flag.IntVar(&args.m, "m", 300, "number of series to send per iteration")
	flag.IntVar(&args.maxStrLen, "max-str-len", 32, "maximum string length")
	flag.BoolVar(&args.viewCode, "view-code", false, "open generated source files in Visual Studio Code")
	flag.BoolVar(&args.keepTemp, "keep-temp", false, "do not remove generated temporary files")
	flag.BoolVar(&args.zeroTime, "zero-time", false, "do not compare timestamps")
	flag.StringVar(&args.network, "network", "udp", "either udp or tcp")
	flag.Parse()
	os.Exit(run(args))
}

func run(args argv) int {
	actualC := make(chan series)
	cancel, err := listen(args, actualC)
	if err != nil {
		log.Fatal(err)
	}
	defer cancel()
	data := newTestData(args)
	expected := data.toSeries(args)
	lib, cancel, err := loadLibraryFromWD()
	if err != nil {
		return fail(err)
	}
	if lib != nil {
		defer cancel()
		return testClient(args, lib, data, expected, actualC)
	}
	var failCount int
	failCount += testClientT[cpp](args, data, expected, actualC)
	failCount += testClientT[golang](args, data, expected, actualC)
	failCount += testClientT[java](args, data, expected, actualC)
	failCount += testClientT[php](args, data, expected, actualC)
	failCount += testClientT[python](args, data, expected, actualC)
	failCount += testClientT[rust](args, data, expected, actualC)
	return failCount
}

func testClient(args argv, lib *library, data any, expected series, actualC chan series) int {
	var failCount int
	for k, v := range testTemplates(lib) {
		log.Printf("*** %s ***\n", k)
		if err := runClient(args, lib, v, data); err != nil {
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

func testClientT[T any](args argv, data any, expected series, actualC chan series) int {
	lib, cancel, err := loadLibrary[T]("")
	if err != nil {
		return fail(err)
	}
	defer cancel()
	return testClient(args, lib, data, expected, actualC)
}

func fail(v ...any) int {
	log.Println(v...)
	log.Println("*** FAILED ***")
	return 1
}
