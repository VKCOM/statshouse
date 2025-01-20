package main

import (
	"github.com/vkcom/statshouse/internal/loadgen"
	"os"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "agent" {
		loadgen.RunAgentLoad()
		return
	}
	loadgen.RunLegacy()
}
