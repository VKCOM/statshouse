// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package grafanaexport

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
)

type exporter struct {
	metaFn     MetaFunc
	cache      map[string]*Meta
	warn       *warnings
	datasource Datasource
}

func (e *exporter) meta(metric string) *Meta {
	if m, ok := e.cache[metric]; ok {
		return m
	}
	var m *Meta
	if e.metaFn != nil {
		m = e.metaFn(metric)
	}
	e.cache[metric] = m
	return m
}

// Export converts a saved StatsHouse dashboard. The output is deterministic for the same input and metadata.
func Export(in Input, meta MetaFunc) Result {
	w := &warnings{}
	e := &exporter{metaFn: meta, cache: map[string]*Meta{}, warn: w, datasource: in.Datasource}
	if len(in.SearchParams) == 0 {
		w.add("the dashboard has no search params (saved by an old version?), nothing was exported")
	}
	m := decode(in, w)

	translatable := map[string]*plot{}
	for _, p := range m.plots {
		switch {
		case p.eventPlot:
			w.addPlot(p, "event plots are not translated, panel omitted")
		case p.metric == "":
			w.addPlot(p, "StatsHouse PromQL plots are not translated, panel omitted")
		default:
			translatable[p.id] = p
		}
	}
	templating, bindings := e.buildVariables(m, translatable)

	type member struct {
		p *plot
		q *panelQuery
	}
	members := map[string][]member{}
	for _, p := range m.plots {
		if translatable[p.id] == nil {
			continue
		}
		q := e.buildPlot(p, bindings[p.id])
		if len(q.targets) == 0 {
			w.addPlot(p, "no query could be built, panel omitted")
			continue
		}
		if m.groups[p.group] == nil {
			m.groups[p.group] = &group{id: p.group, show: true}
			m.groupOrder = append(m.groupOrder, p.group)
		}
		members[p.group] = append(members[p.group], member{p, q})
	}

	var live []*group
	for _, id := range m.groupOrder {
		if len(members[id]) != 0 {
			live = append(live, m.groups[id])
		}
	}
	single := len(live) == 1 && live[0].name == "" && live[0].show

	panels := make([]any, 0)
	nextID, base := 0, 0
	for gi, g := range live {
		var items []member
		var cells []cell
		for _, mem := range members[g.id] {
			items = append(items, mem)
			cells = append(cells, cell{layout: mem.p.layout})
		}
		placed := place(cells)

		origin := base
		if !single {
			origin = base + 1
		}
		var gpanels []Panel
		bottom := 0
		for i, c := range placed {
			mem := items[c.index]
			_ = i
			bottom = max(bottom, c.y+c.h)
			nextID++
			unit := mem.q.unit
			if len(mem.q.units) == 1 {
				unit = mem.q.units[0]
			}
			gpanels = append(gpanels, Panel{
				Type:        "timeseries",
				Title:       mem.p.displayTitle(),
				ID:          nextID,
				Description: mem.p.description,
				Datasource:  e.datasource,
				GridPos:     GridPos{H: c.h, W: c.w, X: c.x, Y: origin + c.y},
				Interval:    mem.q.interval,
				FieldConfig: FieldConfig{
					Defaults:  FieldDefaults{Unit: unit, Custom: FieldCustom{FillOpacity: 10, ShowPoints: "never"}},
					Overrides: []struct{}{},
				},
				Options: Options{
					Legend:  Legend{DisplayMode: "list", Placement: "bottom", ShowLegend: true},
					Tooltip: Tooltip{Mode: "multi", Sort: "desc"},
				},
				Targets: mem.q.targets,
			})
		}
		if single {
			for _, p := range gpanels {
				panels = append(panels, p)
			}
			base += bottom
			continue
		}
		title := g.name
		if title == "" {
			title = fmt.Sprintf("Group %d", gi+1)
		}
		nextID++
		row := Row{Type: "row", Title: title, ID: nextID, Collapsed: !g.show, Panels: []Panel{}, GridPos: GridPos{H: 1, W: gridColumns, Y: base}}
		if g.show {
			panels = append(panels, row)
			for _, p := range gpanels {
				panels = append(panels, p)
			}
			base += 1 + bottom
		} else {
			row.Panels = gpanels
			panels = append(panels, row)
			base++
		}
	}

	title := m.name
	if title == "" {
		title = "StatsHouse dashboard"
	}
	from, to := grafanaTimeRange(m.from, m.to)
	if w.list == nil {
		w.list = []Warning{}
	}
	return Result{
		Dashboard: Dashboard{
			Title:         title,
			Description:   m.description,
			SchemaVersion: schemaVersion,
			Editable:      true,
			Timezone:      "browser",
			Time:          TimeRange{From: from, To: to},
			Templating:    Templating{List: templating},
			Panels:        panels,
		},
		Warnings: w.list,
	}
}

var nonWordRe = regexp.MustCompile(`\W`)

// buildVariables turns dashboard variables into Grafana query variables over label_values of the first linked plot.
// It returns the bindings by plot id and tag.
func (e *exporter) buildVariables(m *model, plots map[string]*plot) ([]Variable, map[string]map[string]varBinding) {
	list := []Variable{}
	bindings := map[string]map[string]varBinding{}
	used := map[string]bool{}
	for i, v := range m.vars {
		var links []link
		for _, l := range v.links {
			if plots[l.plot] != nil {
				links = append(links, l)
			}
		}
		if len(links) == 0 {
			e.warn.add("variable %q is not linked to any translated plot, skipped", v.name)
			continue
		}
		name := nonWordRe.ReplaceAllString(v.name, "_")
		if name == "" {
			name = "var" + strconv.Itoa(i)
		}
		for used[name] {
			name += "_"
		}
		used[name] = true

		first := links[0]
		p := plots[first.plot]
		meta := e.meta(p.metric)
		series := p.metric
		if shapeOf(meta) == shapeSummary {
			series += "_count"
		}
		definition := "label_values(" + selector(series, nil) + ", " + tagLabel(meta, first.tag) + ")"

		current := VariableValue{Text: []string{"All"}, Value: []string{"$__all"}}
		if len(v.values) != 0 {
			vals := make([]string, len(v.values))
			for j, x := range v.values {
				vals[j] = tagValue(x)
			}
			current = VariableValue{Text: vals, Value: vals}
		}
		// "All" of a negated variable must exclude nothing: use a regexp that matches no value.
		allValue := ".*"
		if v.negate {
			allValue = `\b\B`
		}
		list = append(list, Variable{
			Name:       name,
			Label:      v.name,
			Type:       "query",
			Datasource: e.datasource,
			Multi:      true,
			IncludeAll: true,
			AllValue:   allValue,
			Current:    current,
			Options:    []struct{}{},
			Query:      VariableQuery{QryType: 1, Query: definition, RefID: "VariableQueryEditor-VariableQuery"},
			Definition: definition,
			Refresh:    2,
			Sort:       1,
		})
		for _, l := range links {
			if bindings[l.plot] == nil {
				bindings[l.plot] = map[string]varBinding{}
			}
			bindings[l.plot][l.tag] = varBinding{name: name, negate: v.negate, group: v.group}
		}
	}
	return list, bindings
}

type cell struct {
	layout
	index int
}

// place puts cells on the grid in reading order, pushing a cell down until it no longer overlaps a placed one.
func place(cells []cell) []cell {
	for i := range cells {
		cells[i].index = i
	}
	sort.SliceStable(cells, func(i, j int) bool {
		a, b := cells[i], cells[j]
		if a.y != b.y {
			return a.y < b.y
		}
		return a.x < b.x
	})
	for i := range cells {
		c := &cells[i]
		c.w = max(1, min(c.w, gridColumns))
		c.x = max(0, min(c.x, gridColumns-c.w))
		c.h = max(1, c.h)
		for overlaps(c, cells[:i]) {
			c.y++
		}
	}
	return cells
}

func overlaps(c *cell, placed []cell) bool {
	for _, p := range placed {
		if c.x < p.x+p.w && p.x < c.x+c.w && c.y < p.y+p.h && p.y < c.y+c.h {
			return true
		}
	}
	return false
}

// relative returns a Grafana date-math expression for base shifted by sec (<= 0) seconds.
func relative(base string, sec int64) string {
	if sec >= 0 {
		return base
	}
	sec = -sec
	for _, u := range []struct {
		size int64
		sfx  string
	}{{86400, "d"}, {3600, "h"}, {60, "m"}} {
		if sec%u.size == 0 {
			return fmt.Sprintf("%s-%d%s", base, sec/u.size, u.sfx)
		}
	}
	return fmt.Sprintf("%s-%ds", base, sec)
}

// grafanaTimeRange converts the StatsHouse time range: "to" is 0 (now), "ed"/"ew" (end of day/week), a negative
// offset from now or an absolute Unix time; "from" is an offset from "to" or an absolute Unix time.
func grafanaTimeRange(from, to string) (string, string) {
	fromSec, err := strconv.ParseInt(from, 10, 64)
	if err != nil {
		fromSec = -7200
	}
	toExpr, anchor := "now", "now"
	var toAbs, toRel int64
	switch to {
	case "ed":
		toExpr, anchor = "now/d", "now/d+1d"
	case "ew":
		toExpr, anchor = "now/w", "now/w+1w"
	default:
		n, err := strconv.ParseInt(to, 10, 64)
		switch {
		case err != nil || n == 0:
		case n > 0:
			toAbs = n
			toExpr = strconv.FormatInt(n*1000, 10)
		default:
			toRel = n
			toExpr = relative("now", n)
		}
	}
	switch {
	case fromSec > 0:
		return strconv.FormatInt(fromSec*1000, 10), toExpr
	case toAbs > 0:
		return strconv.FormatInt((toAbs+fromSec)*1000, 10), toExpr
	case toRel != 0:
		return relative("now", toRel+fromSec), toExpr
	default:
		return relative(anchor, fromSec), toExpr
	}
}
