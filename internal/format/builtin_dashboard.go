package format

import (
	"encoding/json"
	"fmt"
)

type builtinDashboard struct {
	name     string
	jsonData string
}

var BuiltinDashboardByID = map[int32]*DashboardMeta{}

var builtinDashboardData = map[int32]builtinDashboard{}

func init() {
	for id, data := range builtinDashboardData {
		meta := DashboardMeta{}
		meta.Name = data.name
		meta.DashboardID = id
		err := json.Unmarshal([]byte(data.jsonData), &meta.JSONData)
		if err != nil {
			fmt.Println("can't parse builtin dashboard", data.name)
			continue
		}
		BuiltinDashboardByID[id] = &meta
	}
}
