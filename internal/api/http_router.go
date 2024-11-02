package api

import (
	"fmt"
	"net/http"
	"runtime/debug"
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
	handlerFunc func(*HTTPRequestHandler, *http.Request)
}

type HTTPRequestHandler struct {
	*Handler
	responseWriter http.ResponseWriter
	accessInfo     accessInfo
	endpointStat   endpointStat
	statusCode     int
	statusCodeSent bool
}

type error500 struct {
	time  time.Time
	what  any
	stack []byte
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

func (r *Route) HandlerFunc(f func(*HTTPRequestHandler, *http.Request)) *Route {
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
	w := &HTTPRequestHandler{
		Handler:        r.Handler,
		responseWriter: http,
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

func (r *Route) reportStatistics(w *HTTPRequestHandler) {
	if err := recover(); err != nil {
		if !w.statusCodeSent {
			http.Error(w, fmt.Sprint(err), http.StatusInternalServerError)
		}
		v := error500{time.Now(), err, debug.Stack()}
		w.errorsMu.Lock()
		w.errors[w.errorX] = v
		w.errorX = (w.errorX + 1) % len(w.errors)
		w.errorsMu.Unlock()
	}
	w.endpointStat.report(w.statusCode, format.BuiltinMetricNameAPIResponseTime)
}

func DumpInternalServerErrors(w *HTTPRequestHandler, r *http.Request) {
	if err := w.parseAccessToken(r); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if ok := w.accessInfo.insecureMode || w.accessInfo.bitAdmin; !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.errorsMu.RLock()
	defer w.errorsMu.RUnlock()
	for i := 0; i < len(w.errors) && w.errors[i].what != nil; i++ {
		w.Write([]byte("# \n"))
		w.Write([]byte(fmt.Sprintf("# %s \n", w.errors[i].what)))
		w.Write([]byte("# " + w.errors[i].time.Format(time.RFC3339) + " \n"))
		w.Write([]byte("# \n"))
		w.Write(w.errors[i].stack)
		w.Write([]byte("\n"))
	}
}

func (h *HTTPRequestHandler) Header() http.Header {
	return h.responseWriter.Header()
}

func (h *HTTPRequestHandler) Write(s []byte) (int, error) {
	return h.responseWriter.Write(s)
}

func (h *HTTPRequestHandler) WriteHeader(statusCode int) {
	h.statusCode = statusCode
	h.responseWriter.WriteHeader(statusCode)
	h.statusCodeSent = true
}
