// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package grafanaexport

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/VKCOM/statshouse/internal/format"
)

// removed marks a deleted item in dashboard search params.
const removed = "\x07"

const (
	defaultNumSeries = 5
	defaultWhat      = format.ParamQueryFnCountNorm
	eventPlotType    = "1"
)

var (
	plotKeyRe  = regexp.MustCompile(`^t(\d+)\.`)
	groupKeyRe = regexp.MustCompile(`^g(\d+)\.`)
	varKeyRe   = regexp.MustCompile(`^v(\d+)\.`)
)

type layout struct{ x, y, w, h int }

type plot struct {
	id          string
	metric      string
	promQL      string
	title       string
	description string
	what        []string
	customAgg   int
	groupBy     []string
	filterIn    map[string][]string
	filterNotIn map[string][]string
	numSeries   int
	group       string
	layout      layout
	eventPlot   bool
	eventsShown bool
	maxHost     bool
}

type group struct {
	id   string
	name string
	show bool
}

type link struct{ plot, tag string }

type variable struct {
	name   string
	links  []link
	values []string
	group  bool
	negate bool
}

type model struct {
	name        string
	description string
	from, to    string
	plots       []*plot
	groups      map[string]*group
	groupOrder  []string
	vars        []*variable
}

type searchTree struct {
	keys []string
	vals map[string][]string
}

func newSearchTree(params [][2]string) *searchTree {
	t := &searchTree{vals: make(map[string][]string, len(params))}
	for _, kv := range params {
		if _, ok := t.vals[kv[0]]; !ok {
			t.keys = append(t.keys, kv[0])
		}
		t.vals[kv[0]] = append(t.vals[kv[0]], kv[1])
	}
	return t
}

func (t *searchTree) has(key string) bool { return len(t.vals[key]) != 0 }

func (t *searchTree) str(key, def string) string {
	if v := t.vals[key]; len(v) != 0 {
		return v[0]
	}
	return def
}

func (t *searchTree) list(key string) []string {
	var res []string
	for _, v := range t.vals[key] {
		if v != removed {
			res = append(res, v)
		}
	}
	return res
}

// ids returns the unique numbers captured by re over all keys, in order of first appearance.
func (t *searchTree) ids(re *regexp.Regexp) []string {
	var res []string
	for _, k := range t.keys {
		if m := re.FindStringSubmatch(k); m != nil {
			res = appendUnique(res, m[1])
		}
	}
	return res
}

func appendUnique(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

func digitIDs(s string) []string {
	var res []string
	for _, x := range strings.Split(s, ".") {
		if x != "" && strings.Trim(x, "0123456789") == "" {
			res = append(res, x)
		}
	}
	return res
}

// tagValue decodes a filter value: a leading space marks a raw integer, " 0" is an unset tag.
func tagValue(raw string) string {
	v := strings.TrimSpace(raw)
	if v == "0" && strings.HasPrefix(raw, " ") {
		return ""
	}
	return v
}

func tagSortKey(tag string) int {
	if tag == format.StringTopTagID {
		return math.MaxInt
	}
	i, _ := strconv.Atoi(tag)
	return i
}

func sortedTags[V any](m map[string]V) []string {
	res := make([]string, 0, len(m))
	for k := range m {
		res = append(res, k)
	}
	sort.Slice(res, func(i, j int) bool { return tagSortKey(res[i]) < tagSortKey(res[j]) })
	return res
}

func parseFilters(values []string, p *plot, w *warnings) (in, notIn map[string][]string) {
	in, notIn = map[string][]string{}, map[string][]string{}
	for _, f := range values {
		inIx, notInIx := strings.Index(f, "-"), strings.Index(f, "~")
		dst, sep := in, inIx
		if inIx == -1 || (notInIx != -1 && notInIx < inIx) {
			dst, sep = notIn, notInIx
		}
		if sep == -1 {
			w.addPlot(p, "filter %q is malformed and was ignored", f)
			continue
		}
		tag, err := format.APICompatNormalizeTagID(f[:sep])
		if err != nil {
			w.addPlot(p, "filter on unknown tag %q was ignored", f[:sep])
			continue
		}
		if v := f[sep+1:]; v != "" && v != removed {
			dst[tag] = appendUnique(dst[tag], v)
		}
	}
	return in, notIn
}

func parseLayout(s string) layout {
	l := []int{0, 0, 12, 8}
	for i, x := range strings.Split(s, ".") {
		if v, err := strconv.Atoi(x); err == nil && i < len(l) {
			l[i] = v
		}
	}
	return layout{l[0], l[1], l[2], l[3]}
}

// decode reads the dashboard state from search params (URL format v4): plot 0 uses bare keys, plot N uses "t<N>." prefix.
func decode(in Input, w *warnings) *model {
	t := newSearchTree(in.SearchParams)
	pget := func(pid, key string) string {
		if pid != "0" {
			key = "t" + pid + "." + key
		}
		return key
	}

	plotIDs := t.ids(plotKeyRe)
	if t.has("s") || t.has("q") {
		plotIDs = append([]string{"0"}, removeID(plotIDs, "0")...)
	}
	order := make([]string, 0, len(plotIDs))
	for _, id := range append(digitIDs(t.str("op", "")), plotIDs...) {
		order = appendUnique(order, id)
	}

	plots := map[string]*plot{}
	for _, pid := range plotIDs {
		metric, promQL := t.str(pget(pid, "s"), ""), t.str(pget(pid, "q"), "")
		if metric == removed || (metric == "" && promQL == "") {
			continue
		}
		p := &plot{
			id:          pid,
			metric:      metric,
			promQL:      promQL,
			title:       strings.TrimSpace(t.str(pget(pid, "cn"), "")),
			description: strings.TrimSpace(t.str(pget(pid, "cd"), "")),
			group:       t.str(pget(pid, "dg"), "0"),
			layout:      parseLayout(t.str(pget(pid, "dl"), "")),
			eventPlot:   t.str(pget(pid, "qt"), "0") == eventPlotType,
			eventsShown: t.has(pget(pid, "qe")),
			maxHost:     t.str(pget(pid, "mh"), "0") == "1",
		}
		p.what = t.list(pget(pid, "qw"))
		if len(p.what) == 0 {
			p.what = []string{defaultWhat}
		}
		p.customAgg, _ = strconv.Atoi(t.str(pget(pid, "g"), "0"))
		p.numSeries = defaultNumSeries
		if p.eventPlot {
			p.numSeries = 0
		}
		if n, err := strconv.Atoi(t.str(pget(pid, "n"), "")); err == nil {
			p.numSeries = n
		}
		for _, raw := range t.list(pget(pid, "qb")) {
			tag, err := format.APICompatNormalizeTagID(raw)
			if err != nil {
				w.addPlot(p, "group by unknown tag %q was ignored", raw)
				continue
			}
			p.groupBy = appendUnique(p.groupBy, tag)
		}
		p.filterIn, p.filterNotIn = parseFilters(t.list(pget(pid, "qf")), p, w)
		plots[pid] = p
	}

	m := &model{
		name:        t.str("dn", in.Name),
		description: t.str("dd", in.Description),
		from:        t.str("f", ""),
		to:          t.str("t", ""),
		groups:      map[string]*group{},
	}
	if m.name == "" {
		m.name = in.Name
	}
	for _, id := range order {
		if p := plots[id]; p != nil {
			m.plots = append(m.plots, p)
		}
	}

	gids := t.ids(groupKeyRe)
	for _, id := range digitIDs(t.str("og", "")) {
		gids = appendUnique(gids, id)
	}
	for _, p := range m.plots {
		gids = appendUnique(gids, p.group)
	}
	for _, id := range gids {
		if t.str("g"+id, "") == removed {
			continue
		}
		m.groups[id] = &group{
			id:   id,
			name: strings.ReplaceAll(t.str("g"+id+".t", ""), removed, ""),
			show: t.str("g"+id+".v", "") != "0",
		}
	}
	for _, id := range append(digitIDs(t.str("og", "")), gids...) {
		if m.groups[id] != nil {
			m.groupOrder = appendUnique(m.groupOrder, id)
		}
	}

	vids := t.ids(varKeyRe)
	vorder := make([]string, 0, len(vids))
	for _, id := range append(digitIDs(t.str("ov", "")), vids...) {
		if t.has("v" + id + ".n") {
			vorder = appendUnique(vorder, id)
		}
	}
	for _, id := range vorder {
		name := t.str("v"+id+".n", "")
		if name == "" || name == removed {
			continue
		}
		v := &variable{
			name:   name,
			values: t.list("v." + name),
			group:  t.str("v."+name+".g", "") == "1",
			negate: t.str("v."+name+".nv", "") == "1",
		}
		for _, x := range strings.Split(t.str("v"+id+".l", ""), "-") {
			pid, tagIx, ok := strings.Cut(x, ".")
			if !ok {
				continue
			}
			ix, err := strconv.Atoi(tagIx)
			switch {
			case err != nil:
				continue
			case ix < 0:
				v.links = append(v.links, link{pid, format.StringTopTagID})
			case ix < format.MaxTags:
				v.links = append(v.links, link{pid, format.TagID(ix)})
			}
		}
		m.vars = append(m.vars, v)
	}
	return m
}

func removeID(ids []string, id string) []string {
	res := make([]string, 0, len(ids))
	for _, x := range ids {
		if x != id {
			res = append(res, x)
		}
	}
	return res
}

func (p *plot) displayTitle() string {
	switch {
	case p.title != "":
		return p.title
	case p.metric != "":
		return fmt.Sprintf("%s: %s", p.metric, strings.Join(p.what, ","))
	default:
		return "promQL"
	}
}
