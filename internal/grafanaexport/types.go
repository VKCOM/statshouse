// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package grafanaexport

import "fmt"

// Input is a saved StatsHouse dashboard; SearchParams is data.searchParams of GET /api/dashboard.
type Input struct {
	Datasource   Datasource // VictoriaMetrics datasource all panels and variables refer to
	Name         string
	Description  string
	SearchParams [][2]string
}

// Meta is the part of StatsHouse metric metadata the export needs.
type Meta struct {
	Kind          string   // format.MetricKind*
	TagNames      []string // custom tag names by tag index, "" for default
	StringTopName string
	MetricType    string // format.Metric*
}

// MetaFunc returns metadata of a metric, or nil if it is unknown or not viewable.
type MetaFunc func(metric string) *Meta

type Result struct {
	Dashboard Dashboard `json:"dashboard"`
	Warnings  []Warning `json:"warnings"`
}

type Warning struct {
	Plot    string `json:"plot,omitempty"`
	Title   string `json:"title,omitempty"`
	Message string `json:"message"`
}

type warnings struct{ list []Warning }

func (w *warnings) add(format string, args ...any) {
	w.list = append(w.list, Warning{Message: fmt.Sprintf(format, args...)})
}

func (w *warnings) addPlot(p *plot, format string, args ...any) {
	w.list = append(w.list, Warning{Plot: p.id, Title: p.displayTitle(), Message: fmt.Sprintf(format, args...)})
}

type Dashboard struct {
	Title         string     `json:"title"`
	Description   string     `json:"description,omitempty"`
	SchemaVersion int        `json:"schemaVersion"`
	Editable      bool       `json:"editable"`
	Timezone      string     `json:"timezone"`
	Time          TimeRange  `json:"time"`
	Templating    Templating `json:"templating"`
	Panels        []any      `json:"panels"` // Panel and Row
}

type TimeRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type Templating struct {
	List []Variable `json:"list"`
}

type Datasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type Variable struct {
	Name       string        `json:"name"`
	Label      string        `json:"label"`
	Type       string        `json:"type"`
	Datasource Datasource    `json:"datasource"`
	Multi      bool          `json:"multi"`
	IncludeAll bool          `json:"includeAll"`
	AllValue   string        `json:"allValue"`
	Current    VariableValue `json:"current"`
	Options    []struct{}    `json:"options"`
	Query      VariableQuery `json:"query"`
	Definition string        `json:"definition"`
	Refresh    int           `json:"refresh"`
	Sort       int           `json:"sort"`
	Regex      string        `json:"regex"`
}

type VariableValue struct {
	Text  []string `json:"text"`
	Value []string `json:"value"`
}

type VariableQuery struct {
	QryType int    `json:"qryType"`
	Query   string `json:"query"`
	RefID   string `json:"refId"`
}

type GridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

type Row struct {
	Type      string  `json:"type"`
	Title     string  `json:"title"`
	ID        int     `json:"id"`
	Collapsed bool    `json:"collapsed"`
	Panels    []Panel `json:"panels"`
	GridPos   GridPos `json:"gridPos"`
}

type Panel struct {
	Type        string      `json:"type"`
	Title       string      `json:"title"`
	ID          int         `json:"id"`
	Description string      `json:"description,omitempty"`
	Datasource  Datasource  `json:"datasource"`
	GridPos     GridPos     `json:"gridPos"`
	Interval    string      `json:"interval,omitempty"`
	FieldConfig FieldConfig `json:"fieldConfig"`
	Options     Options     `json:"options"`
	Targets     []Target    `json:"targets"`
}

type FieldConfig struct {
	Defaults  FieldDefaults `json:"defaults"`
	Overrides []struct{}    `json:"overrides"`
}

type FieldDefaults struct {
	Unit   string      `json:"unit"`
	Custom FieldCustom `json:"custom"`
}

type FieldCustom struct {
	FillOpacity int    `json:"fillOpacity"`
	ShowPoints  string `json:"showPoints"`
}

type Options struct {
	Legend  Legend  `json:"legend"`
	Tooltip Tooltip `json:"tooltip"`
}

type Legend struct {
	DisplayMode string `json:"displayMode"`
	Placement   string `json:"placement"`
	ShowLegend  bool   `json:"showLegend"`
}

type Tooltip struct {
	Mode string `json:"mode"`
	Sort string `json:"sort"`
}

type Target struct {
	RefID        string     `json:"refId"`
	Datasource   Datasource `json:"datasource"`
	Expr         string     `json:"expr"`
	EditorMode   string     `json:"editorMode"`
	Range        bool       `json:"range"`
	LegendFormat string     `json:"legendFormat,omitempty"`
}
