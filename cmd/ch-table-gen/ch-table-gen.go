package main

import (
	"bytes"
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

type TableParams struct {
	NamePrefix  string
	NamePostfix string
	Resolution  string
	Cluster     string
	Schema      SchemaParams
	SelectFrom  string
}

type Params struct {
	IncomingTable TableParams
	Tables        []TableParams
}

func (p TableParams) tableName() string {
	return p.NamePrefix + "_" + p.Resolution + "_" + p.NamePostfix
}

func parseSchemaParams() (params SchemaParams) {
	var basicTagsN int
	var rawTagsN int
	var stringTags bool
	flag.IntVar(&basicTagsN, "basic-tags", 32, "number of basic tags")
	flag.BoolVar(&stringTags, "string-tags", true, "basic tags can be stored as unmapped strings")
	flag.IntVar(&rawTagsN, "raw-tags", 4, "number of raw tags")
	flag.BoolVar(&params.HostTag, "host-tag", true, "special host tag")
	flag.Parse()
	params.BasicTags = make([]BasicTagParams, basicTagsN)
	for i := 0; i < basicTagsN; i++ {
		params.BasicTags[i] = BasicTagParams{
			Index:       i,
			StringValue: stringTags,
		}
	}
	params.RawTags = make([]int, rawTagsN)
	for i := 0; i < rawTagsN; i++ {
		params.RawTags[i] = i
	}
	return params
}

func main() {
	schemaParams := parseSchemaParams()
	prekeySchemaParams := schemaParams
	prekeySchemaParams.Prekey = true
	params := Params{
		Tables: []TableParams{
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_basic",
				Resolution:  "1s",
				Cluster:     "local_test_cluster",
				Schema:      schemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
			},
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_prekey",
				Resolution:  "1s",
				Cluster:     "local_test_cluster",
				Schema:      prekeySchemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
			},
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_basic",
				Resolution:  "1m",
				Cluster:     "local_test_cluster",
				Schema:      schemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
			},
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_prekey",
				Resolution:  "1m",
				Cluster:     "local_test_cluster",
				Schema:      prekeySchemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
			},
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_basic",
				Resolution:  "1h",
				Cluster:     "local_test_cluster",
				Schema:      schemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
			},
			{
				NamePrefix:  "statshouse_value",
				NamePostfix: "str_prekey",
				Resolution:  "1h",
				Cluster:     "local_test_cluster",
				Schema:      prekeySchemaParams,
				SelectFrom:  "statshouse_value_incoming_str",
			},
		},
	}

	tmpl, err := template.ParseFiles("init-statshouse.go.tmpl", "resolution-tables.go.tmpl", "table-schema.go.tmpl", "table-order.go.tmpl")
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
