package main

import (
	"fmt"
	"os"

	"github.com/vkcom/statshouse/internal/loadgen"
)

func usage() {
	fmt.Printf("usage: loadgen client|new-pipeline|set-sharding\n")
	os.Exit(1)
}

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "client":
			loadgen.RunClientLoad()
			return
		case "new-pipeline":
			loadgen.RunEnableNewPipeline()
			return
		case "set-sharding":
			loadgen.RunSetSharding()
			return
		}
	}
	usage()
}
