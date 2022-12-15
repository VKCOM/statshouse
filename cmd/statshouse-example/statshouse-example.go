package main

import (
	"flag"
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	"runtime/metrics"
	"strings"
	"time"

	"github.com/vkcom/statshouse-go"
)

const (
	responseTimeMetricName  = "example_response_time"
	runtimeMemoryMetricName = "example_runtime_memory"

	handlerTag = "handler"
	statusTag  = "status"
	classTag   = "class"
)

type handler struct {
	name   string
	shAddr string
	tmpl   *template.Template
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// simulate an asynchronous operation
	sleep := r.FormValue("sleep")
	if sleep != "" {
		d, err := time.ParseDuration(sleep)
		if err != nil {
			log.Printf("failed to parse sleep parameter value: %v", err)
		} else {
			time.Sleep(d)
		}
	}

	// do the actual work
	err := h.tmpl.Execute(w, &templateData{
		Start:               start,
		BaseCSS:             rebootCSS,
		HandlerName:         h.name,
		ResponseTimeMetric:  responseTimeMetricName,
		RuntimeMemoryMetric: runtimeMemoryMetricName,
		HandlerTag:          handlerTag,
		StatusTag:           statusTag,
		ClassTag:            classTag,
		StatsHouseAddress:   h.shAddr,
	})

	recordResponseTime(start, h.name, err)
}

type templateData struct {
	Start               time.Time
	BaseCSS             template.CSS
	HandlerName         string
	ResponseTimeMetric  string
	RuntimeMemoryMetric string
	HandlerTag          string
	StatusTag           string
	ClassTag            string
	StatsHouseAddress   string
}

func (d *templateData) Elapsed() time.Duration {
	return time.Since(d.Start)
}

func run(addr string, shAddr string) error {
	id := statshouse.StartRegularMeasurement(recordRegular)
	defer statshouse.StopRegularMeasurement(id)

	tmpl, err := template.New("example").Parse(html)
	if err != nil {
		return fmt.Errorf("failed to parse HTML template: %w", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to create listening socket: %w", err)
	}
	defer func() { _ = ln.Close() }()
	log.Printf("listening on http://%s", ln.Addr().String())

	http.Handle("/foo", &handler{name: "/foo", shAddr: shAddr, tmpl: tmpl})
	http.Handle("/bar", &handler{name: "/bar", shAddr: shAddr, tmpl: tmpl})
	http.Handle("/", &handler{name: "/", shAddr: shAddr, tmpl: tmpl})

	return http.Serve(ln, nil) // omit graceful shutdown for brevity
}

func main() {
	var (
		addr   = flag.String("listen", "localhost:3333", "HTTP server listen address")
		shAddr = flag.String("statshouse", "localhost:10888", "StatsHouse HTTP API/UI address")
	)
	flag.Parse()

	err := run(*addr, *shAddr)
	if err != nil {
		log.Fatal(err)
	}
}

func recordResponseTime(start time.Time, handler string, err error) {
	status := "ok"
	if err != nil {
		status = "error"
	}

	statshouse.AccessMetric(responseTimeMetricName,
		statshouse.Tags{
			{handlerTag, handler},
			{statusTag, status},
		}).
		Value(time.Since(start).Seconds())
}

func recordRegular(r *statshouse.Registry) {
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/free:bytes"},
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/memory/classes/heap/stacks:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/memory/classes/metadata/mcache/free:bytes"},
		{Name: "/memory/classes/metadata/mcache/inuse:bytes"},
		{Name: "/memory/classes/metadata/mspan/free:bytes"},
		{Name: "/memory/classes/metadata/mspan/inuse:bytes"},
		{Name: "/memory/classes/metadata/other:bytes"},
		{Name: "/memory/classes/os-stacks:bytes"},
		{Name: "/memory/classes/other:bytes"},
		{Name: "/memory/classes/profiling/buckets:bytes"},
	}
	metrics.Read(samples)
	for _, s := range samples {
		c := strings.TrimSuffix(strings.TrimPrefix(s.Name, "/memory/classes"), ":bytes")
		m := r.AccessMetric(runtimeMemoryMetricName, statshouse.Tags{{classTag, c}})
		m.Value(float64(s.Value.Uint64()))
	}
}

const (
	// language=gotemplate
	html = `<!DOCTYPE html>
<html lang="en">
<head>
  <title>StatsHouse Client Example</title>
  <style>{{ .BaseCSS }}</style>
  <style>
    body {
      max-width: 720px;
      margin: 1rem auto;
    }
  </style>
</head>
<body>
  <h1>StatsHouse Client Example</h1>
  <p>
    This page is served by an example Go web server instrumented with
    <a href="https://pkg.go.dev/github.com/vkcom/statshouse-go" target="_blank">Go client library</a>.
  </p>
  <p>
    Every second, <code>{{ .RuntimeMemoryMetric }}</code> metric is recorded,
    with a <code>{{ .ClassTag }}</code> tag.
  </p>
  <p>
    Each time a page is served, <code>{{ .ResponseTimeMetric }}</code> metric is recorded,
    with <code>{{ .HandlerTag }}</code> and <code>{{ .StatusTag}}</code> tags. Run your favorite
    load testing tool (like ApacheBench, wrk, wrk2, siege or vegeta) to see how much this tiny web server can handle.
  </p>
  <ul>
    <li>Page handler: <code>{{ .HandlerName }}</code></li>
    <li>Page render time: {{ .Elapsed }}</li>
  </ul>
  <p>
    Watch the metrics live: <a href="http://{{ .StatsHouseAddress }}/view?live&f=-300&t=0&tn=-1&s={{ .ResponseTimeMetric }}&t1.s={{ .ResponseTimeMetric }}&t1.qw=avg&t2.s={{ .RuntimeMemoryMetric }}&t2.qw=avg&t2.qb=key1" target="_blank">dashboard</a>.
  </p>
</body>
</html>
`

	// language=css
	rebootCSS = `
/*!
 * Bootstrap Reboot v5.2.3 (https://getbootstrap.com/)
 * Copyright 2011-2022 The Bootstrap Authors
 * Copyright 2011-2022 Twitter, Inc.
 * Licensed under MIT (https://github.com/twbs/bootstrap/blob/main/LICENSE)
 */
:root {
  --bs-blue: #0d6efd;
  --bs-indigo: #6610f2;
  --bs-purple: #6f42c1;
  --bs-pink: #d63384;
  --bs-red: #dc3545;
  --bs-orange: #fd7e14;
  --bs-yellow: #ffc107;
  --bs-green: #198754;
  --bs-teal: #20c997;
  --bs-cyan: #0dcaf0;
  --bs-black: #000;
  --bs-white: #fff;
  --bs-gray: #6c757d;
  --bs-gray-dark: #343a40;
  --bs-gray-100: #f8f9fa;
  --bs-gray-200: #e9ecef;
  --bs-gray-300: #dee2e6;
  --bs-gray-400: #ced4da;
  --bs-gray-500: #adb5bd;
  --bs-gray-600: #6c757d;
  --bs-gray-700: #495057;
  --bs-gray-800: #343a40;
  --bs-gray-900: #212529;
  --bs-primary: #0d6efd;
  --bs-secondary: #6c757d;
  --bs-success: #198754;
  --bs-info: #0dcaf0;
  --bs-warning: #ffc107;
  --bs-danger: #dc3545;
  --bs-light: #f8f9fa;
  --bs-dark: #212529;
  --bs-primary-rgb: 13, 110, 253;
  --bs-secondary-rgb: 108, 117, 125;
  --bs-success-rgb: 25, 135, 84;
  --bs-info-rgb: 13, 202, 240;
  --bs-warning-rgb: 255, 193, 7;
  --bs-danger-rgb: 220, 53, 69;
  --bs-light-rgb: 248, 249, 250;
  --bs-dark-rgb: 33, 37, 41;
  --bs-white-rgb: 255, 255, 255;
  --bs-black-rgb: 0, 0, 0;
  --bs-body-color-rgb: 33, 37, 41;
  --bs-body-bg-rgb: 255, 255, 255;
  --bs-font-sans-serif: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", "Noto Sans", "Liberation Sans", Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol", "Noto Color Emoji";
  --bs-font-monospace: SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  --bs-gradient: linear-gradient(180deg, rgba(255, 255, 255, 0.15), rgba(255, 255, 255, 0));
  --bs-body-font-family: var(--bs-font-sans-serif);
  --bs-body-font-size: 1rem;
  --bs-body-font-weight: 400;
  --bs-body-line-height: 1.5;
  --bs-body-color: #212529;
  --bs-body-bg: #fff;
  --bs-border-width: 1px;
  --bs-border-style: solid;
  --bs-border-color: #dee2e6;
  --bs-border-color-translucent: rgba(0, 0, 0, 0.175);
  --bs-border-radius: 0.375rem;
  --bs-border-radius-sm: 0.25rem;
  --bs-border-radius-lg: 0.5rem;
  --bs-border-radius-xl: 1rem;
  --bs-border-radius-2xl: 2rem;
  --bs-border-radius-pill: 50rem;
  --bs-link-color: #0d6efd;
  --bs-link-hover-color: #0a58ca;
  --bs-code-color: #d63384;
  --bs-highlight-bg: #fff3cd;
}

*,
*::before,
*::after {
  box-sizing: border-box;
}

@media (prefers-reduced-motion: no-preference) {
  :root {
    scroll-behavior: smooth;
  }
}

body {
  margin: 0;
  font-family: var(--bs-body-font-family);
  font-size: var(--bs-body-font-size);
  font-weight: var(--bs-body-font-weight);
  line-height: var(--bs-body-line-height);
  color: var(--bs-body-color);
  background-color: var(--bs-body-bg);
  -webkit-text-size-adjust: 100%;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
}

hr {
  margin: 1rem 0;
  color: inherit;
  border: 0;
  border-top: 1px solid;
  opacity: 0.25;
}

h6, h5, h4, h3, h2, h1 {
  margin-top: 0;
  margin-bottom: 0.5rem;
  font-weight: 500;
  line-height: 1.2;
}

h1 {
  font-size: calc(1.375rem + 1.5vw);
}
@media (min-width: 1200px) {
  h1 {
    font-size: 2.5rem;
  }
}

h2 {
  font-size: calc(1.325rem + 0.9vw);
}
@media (min-width: 1200px) {
  h2 {
    font-size: 2rem;
  }
}

h3 {
  font-size: calc(1.3rem + 0.6vw);
}
@media (min-width: 1200px) {
  h3 {
    font-size: 1.75rem;
  }
}

h4 {
  font-size: calc(1.275rem + 0.3vw);
}
@media (min-width: 1200px) {
  h4 {
    font-size: 1.5rem;
  }
}

h5 {
  font-size: 1.25rem;
}

h6 {
  font-size: 1rem;
}

p {
  margin-top: 0;
  margin-bottom: 1rem;
}

abbr[title] {
  -webkit-text-decoration: underline dotted;
  text-decoration: underline dotted;
  cursor: help;
  -webkit-text-decoration-skip-ink: none;
  text-decoration-skip-ink: none;
}

address {
  margin-bottom: 1rem;
  font-style: normal;
  line-height: inherit;
}

ol,
ul {
  padding-left: 2rem;
}

ol,
ul,
dl {
  margin-top: 0;
  margin-bottom: 1rem;
}

ol ol,
ul ul,
ol ul,
ul ol {
  margin-bottom: 0;
}

dt {
  font-weight: 700;
}

dd {
  margin-bottom: 0.5rem;
  margin-left: 0;
}

blockquote {
  margin: 0 0 1rem;
}

b,
strong {
  font-weight: bolder;
}

small {
  font-size: 0.875em;
}

mark {
  padding: 0.1875em;
  background-color: var(--bs-highlight-bg);
}

sub,
sup {
  position: relative;
  font-size: 0.75em;
  line-height: 0;
  vertical-align: baseline;
}

sub {
  bottom: -0.25em;
}

sup {
  top: -0.5em;
}

a {
  color: var(--bs-link-color);
  text-decoration: underline;
}
a:hover {
  color: var(--bs-link-hover-color);
}

a:not([href]):not([class]), a:not([href]):not([class]):hover {
  color: inherit;
  text-decoration: none;
}

pre,
code,
kbd,
samp {
  font-family: var(--bs-font-monospace);
  font-size: 1em;
}

pre {
  display: block;
  margin-top: 0;
  margin-bottom: 1rem;
  overflow: auto;
  font-size: 0.875em;
}
pre code {
  font-size: inherit;
  color: inherit;
  word-break: normal;
}

code {
  font-size: 0.875em;
  color: var(--bs-code-color);
  word-wrap: break-word;
}
a > code {
  color: inherit;
}

kbd {
  padding: 0.1875rem 0.375rem;
  font-size: 0.875em;
  color: var(--bs-body-bg);
  background-color: var(--bs-body-color);
  border-radius: 0.25rem;
}
kbd kbd {
  padding: 0;
  font-size: 1em;
}

figure {
  margin: 0 0 1rem;
}

img,
svg {
  vertical-align: middle;
}

table {
  caption-side: bottom;
  border-collapse: collapse;
}

caption {
  padding-top: 0.5rem;
  padding-bottom: 0.5rem;
  color: #6c757d;
  text-align: left;
}

th {
  text-align: inherit;
  text-align: -webkit-match-parent;
}

thead,
tbody,
tfoot,
tr,
td,
th {
  border-color: inherit;
  border-style: solid;
  border-width: 0;
}

label {
  display: inline-block;
}

button {
  border-radius: 0;
}

button:focus:not(:focus-visible) {
  outline: 0;
}

input,
button,
select,
optgroup,
textarea {
  margin: 0;
  font-family: inherit;
  font-size: inherit;
  line-height: inherit;
}

button,
select {
  text-transform: none;
}

[role=button] {
  cursor: pointer;
}

select {
  word-wrap: normal;
}
select:disabled {
  opacity: 1;
}

[list]:not([type=date]):not([type=datetime-local]):not([type=month]):not([type=week]):not([type=time])::-webkit-calendar-picker-indicator {
  display: none !important;
}

button,
[type=button],
[type=reset],
[type=submit] {
  -webkit-appearance: button;
}
button:not(:disabled),
[type=button]:not(:disabled),
[type=reset]:not(:disabled),
[type=submit]:not(:disabled) {
  cursor: pointer;
}

::-moz-focus-inner {
  padding: 0;
  border-style: none;
}

textarea {
  resize: vertical;
}

fieldset {
  min-width: 0;
  padding: 0;
  margin: 0;
  border: 0;
}

legend {
  float: left;
  width: 100%;
  padding: 0;
  margin-bottom: 0.5rem;
  font-size: calc(1.275rem + 0.3vw);
  line-height: inherit;
}
@media (min-width: 1200px) {
  legend {
    font-size: 1.5rem;
  }
}
legend + * {
  clear: left;
}

::-webkit-datetime-edit-fields-wrapper,
::-webkit-datetime-edit-text,
::-webkit-datetime-edit-minute,
::-webkit-datetime-edit-hour-field,
::-webkit-datetime-edit-day-field,
::-webkit-datetime-edit-month-field,
::-webkit-datetime-edit-year-field {
  padding: 0;
}

::-webkit-inner-spin-button {
  height: auto;
}

[type=search] {
  outline-offset: -2px;
  -webkit-appearance: textfield;
}

/* rtl:raw:
[type="tel"],
[type="url"],
[type="email"],
[type="number"] {
  direction: ltr;
}
*/
::-webkit-search-decoration {
  -webkit-appearance: none;
}

::-webkit-color-swatch-wrapper {
  padding: 0;
}

::-webkit-file-upload-button {
  font: inherit;
  -webkit-appearance: button;
}

::file-selector-button {
  font: inherit;
  -webkit-appearance: button;
}

output {
  display: inline-block;
}

iframe {
  border: 0;
}

summary {
  display: list-item;
  cursor: pointer;
}

progress {
  vertical-align: baseline;
}

[hidden] {
  display: none !important;
}
`
)
