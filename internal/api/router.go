package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/vkcom/statshouse/internal/format"
)

type Router struct {
	*Handler
	*mux.Router
}

type Route struct {
	*Handler
	*mux.Route
	endpoint    string
	handlerFunc func(*ResponseWriter, *http.Request)
}

type ResponseWriter struct {
	*Handler
	http           http.ResponseWriter
	endpointStat   endpointStat
	statusCode     int
	statusCodeSent bool
}

func (r Router) Path(tpl string) *Route {
	return &Route{
		Handler:  r.Handler,
		Route:    r.Router.Path(tpl),
		endpoint: tpl[strings.LastIndex(tpl, "/")+1:],
	}
}

func (r Router) PathPrefix(tpl string) *Route {
	return &Route{
		Handler: r.Handler,
		Route:   r.Router.PathPrefix(tpl),
	}
}

func (r *Route) Subrouter() Router {
	return Router{
		Handler: r.Handler,
		Router:  r.Route.Subrouter(),
	}
}

func (r *Route) Methods(methods ...string) *Route {
	r.Route = r.Route.Methods(methods...)
	return r
}

func (r *Route) HandlerFunc(f func(*ResponseWriter, *http.Request)) *Route {
	r.handlerFunc = f
	r.Route.HandlerFunc(r.handle)
	return r
}

func (r *Route) handle(http http.ResponseWriter, req *http.Request) {
	timeNow := time.Now()
	var metric string
	if v := req.FormValue(ParamMetric); v != "" {
		if metricID := r.getMetricIDForStat(v); metricID != 0 {
			metric = strconv.Itoa(int(metricID))
		}
	}
	var dataFormat string
	if v := req.FormValue(paramDataFormat); v != "" {
		dataFormat = v
	} else {
		dataFormat = "json"
	}
	w := &ResponseWriter{
		Handler: r.Handler,
		http:    http,
		endpointStat: endpointStat{
			timestamp:  timeNow,
			endpoint:   r.endpoint,
			protocol:   format.TagValueIDHTTP,
			method:     req.Method,
			dataFormat: dataFormat,
			metric:     metric,
			priority:   req.FormValue(paramPriority),
			timings: ServerTimingHeader{
				Timings: make(map[string][]time.Duration),
				started: timeNow,
			},
		},
	}
	defer r.reportStatistics(w)
	r.handlerFunc(w, req)
}

func (r *Route) reportStatistics(w *ResponseWriter) {
	if err := recover(); err != nil {
		if !w.statusCodeSent {
			http.Error(w, fmt.Sprint(err), http.StatusInternalServerError)
		}
	}
	w.endpointStat.report(w.statusCode, format.BuiltinMetricNameAPIResponseTime)
}

func (w *ResponseWriter) Header() http.Header {
	return w.http.Header()
}

func (w *ResponseWriter) Write(s []byte) (int, error) {
	return w.http.Write(s)
}

func (w *ResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.http.WriteHeader(statusCode)
	w.statusCodeSent = true
}
