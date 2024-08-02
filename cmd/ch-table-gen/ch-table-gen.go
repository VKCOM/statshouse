package main

import (
	"bytes"
	"embed"
	"flag"
	"fmt"
	"log"
	"text/template"
)

type BasicTagParams struct {
	Index       int
	StringValue bool
}

type SchemaParams struct {
	BasicTags []BasicTagParams
	RawTags   []int
	HostTag   bool
	Prekey    bool
	PrekeySet bool
}

type TableTTL struct {
	DaysToDisk int
	DiskName   string
	Days       int
	Hours      int
}

func (t TableTTL) String() string {
	if t.DaysToDisk <= 0 && t.Days <= 0 && t.Hours <= 0 {
		return ""
	}
	if t.Days > 0 && t.Hours > 0 {
		log.Fatalln("can't simultaneously set TTL in days and hours")
	}
	r := "TTL "
	needComma := false
	if t.DaysToDisk > 0 {
		r += fmt.Sprintf("time + toIntervalDay(%d) TO DISK '%s'", t.DaysToDisk, t.DiskName)
		needComma = true
	}
	if t.Days > 0 {
		if needComma {
			r += ", "
		}
		r += fmt.Sprintf("time + toIntervalDay(%d)", t.Days)
	}
	if t.Hours > 0 {
		r += fmt.Sprintf("time + toIntervalHour(%d)", t.Hours)
	}
	return r
}

type TableSettings struct {
	IntSettings map[string]int
	StrSettings map[string]string
}

func (t TableSettings) String() string {
	if len(t.IntSettings) == 0 && len(t.StrSettings) == 0 {
		return ""
	}
	r := "SETTINGS "
	needComma := false
	for k, v := range t.IntSettings {
		if needComma {
			r += ", "
		}
		r += fmt.Sprintf("%s = %d", k, v)
		needComma = true
	}
	for k, v := range t.StrSettings {
		if needComma {
			r += ", "
		}
		r += fmt.Sprintf("%s = '%s'", k, v)
		needComma = true
	}
	return r
}

type TableParams struct {
	NamePrefix  string
	NamePostfix string
	Resolution  string
	Cluster     string
	Schema      SchemaParams
	SelectFrom  string
	TTL         TableTTL
	Settings    TableSettings
}

type IncomingTableParams struct {
	NamePrefix  string
	NamePostfix string
	Cluster     string
	Schema      SchemaParams
}

type Params struct {
	IncomingTable IncomingTableParams
	Tables        []TableParams
}

func parseParams() (params Params) {
	var schemaParams SchemaParams
	var basicTagsN int
	var rawTagsN int
	var stringTags bool
	var cluster string
	flag.IntVar(&basicTagsN, "basic-tags", 32, "number of basic tags")
	flag.BoolVar(&stringTags, "string-tags", true, "basic tags can be stored as unmapped strings")
	flag.IntVar(&rawTagsN, "raw-tags", 4, "number of raw tags")
	flag.BoolVar(&schemaParams.HostTag, "host-tag", true, "special host tag")
	flag.StringVar(&cluster, "cluster", "statlogs2", "clickhouse cluster name")
	flag.Parse()

	schemaParams.BasicTags = make([]BasicTagParams, basicTagsN)
	for i := 0; i < basicTagsN; i++ {
		schemaParams.BasicTags[i] = BasicTagParams{
			Index:       i,
			StringValue: stringTags,
		}
	}
	schemaParams.RawTags = make([]int, rawTagsN)
	for i := 0; i < rawTagsN; i++ {
		schemaParams.RawTags[i] = i
	}

	prekeySchemaParams := schemaParams
	prekeySchemaParams.Prekey = true
	incomingSchemaParams := schemaParams
	incomingSchemaParams.Prekey = true
	incomingSchemaParams.PrekeySet = true
	commonSettings := TableSettings{
		IntSettings: make(map[string]int),
		StrSettings: make(map[string]string),
	}
	commonSettings.IntSettings["index_granularity"] = 8192
	commonSettings.IntSettings["ttl_only_drop_parts"] = 1
	commonSettings.StrSettings["storage_policy"] = "ssd_then_hdd"
	secSettings := TableSettings{
		IntSettings: make(map[string]int),
		StrSettings: make(map[string]string),
	}
	for k, v := range commonSettings.IntSettings {
		secSettings.IntSettings[k] = v
	}
	for k, v := range commonSettings.StrSettings {
		secSettings.StrSettings[k] = v
	}
	secSettings.IntSettings["max_bytes_to_merge_at_max_space_in_pool"] = 16106127360

	params = Params{
		IncomingTable: IncomingTableParams{
			NamePrefix:  "statshouse_value",
			NamePostfix: "incoming_str",
			Cluster:     cluster,
			Schema:      incomingSchemaParams,
		},
		Tables: []TableParams{
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_basic",
				Resolution:  "1s",
				Cluster:     cluster,
				Schema:      schemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
				TTL: TableTTL{
					Hours: 52,
				},
				Settings: secSettings,
			},
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_prekey",
				Resolution:  "1s",
				Cluster:     cluster,
				Schema:      prekeySchemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
				TTL: TableTTL{
					Hours: 52,
				},
				Settings: secSettings,
			},
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_basic",
				Resolution:  "1m",
				Cluster:     cluster,
				Schema:      schemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
				TTL: TableTTL{
					DaysToDisk: 4,
					DiskName:   "default",
					Days:       33,
				},
				Settings: commonSettings,
			},
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_prekey",
				Resolution:  "1m",
				Cluster:     cluster,
				Schema:      prekeySchemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
				TTL: TableTTL{
					DaysToDisk: 4,
					DiskName:   "default",
					Days:       33,
				},
				Settings: commonSettings,
			},
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_basic",
				Resolution:  "1h",
				Cluster:     cluster,
				Schema:      schemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
				TTL: TableTTL{
					DaysToDisk: 4,
					DiskName:   "default",
				},
				Settings: commonSettings,
			},
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_prekey",
				Resolution:  "1h",
				Cluster:     cluster,
				Schema:      prekeySchemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
				Settings:    commonSettings,
			},
		},
	}

	return params
}

//go:embed init-statshouse.go.tmpl resolution-tables.go.tmpl table-schema.go.tmpl table-order.go.tmpl
var embedTemplates embed.FS

func main() {
	params := parseParams()

	tmpl, err := template.ParseFS(embedTemplates, "*.go.tmpl")
	if err != nil {
		log.Fatal("failed to parse template file table.go.tmpl:", err)
	}

	buffer := new(bytes.Buffer)
	err = tmpl.ExecuteTemplate(buffer, "init-statshouse.go.tmpl", params)
	if err != nil {
		log.Fatal("failed to render template:", err)
	}
	fmt.Print(buffer)
}
