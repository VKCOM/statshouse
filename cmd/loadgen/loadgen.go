package main

import (
	"os"

	"github.com/vkcom/statshouse/internal/loadgen"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "client" {
		loadgen.RunClientLoad()
		return
	}
}
