package api

import (
	"fmt"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql"
)

type httpRouter struct {
	*Handler
	*mux.Router
}

type httpRoute struct {
	*Handler
	*mux.Route
	endpoint    string
	handlerFunc func(*httpRequestHandler)
}

type requestHandler struct {
	*Handler
	accessInfo   accessInfo
	endpointStat endpointStat
	trace        []string
	traceMu      sync.Mutex
	debug        bool
	version      string
	query        promql.Query
}

type httpRequestHandler struct {
	requestHandler
	*http.Request
	w httpResponseWriter
}

type httpResponseWriter struct {
	http.ResponseWriter
	handlerErr     error
	statusCode     int
	statusCodeSent bool
}

type error500 struct {
	time       time.Time
	requestURI string
	what       any
	stack      []byte
	trace      []string
}

func NewHTTPRouter(h *Handler) httpRouter {
	return httpRouter{
		Handler: h,
		Router:  mux.NewRouter(),
	}
}

func (r httpRouter) Path(tpl string) *httpRoute {
	return &httpRoute{
		Handler:  r.Handler,
		Route:    r.Router.Path(tpl),
		endpoint: tpl[strings.LastIndex(tpl, "/")+1:],
	}
}

func (r httpRouter) PathPrefix(tpl string) *httpRoute {
	return &httpRoute{
		Handler: r.Handler,
		Route:   r.Router.PathPrefix(tpl),
	}
}

func (r *httpRoute) Subrouter() httpRouter {
	return httpRouter{
		Handler: r.Handler,
		Router:  r.Route.Subrouter(),
	}
}

func (r *httpRoute) Methods(methods ...string) *httpRoute {
	r.Route = r.Route.Methods(methods...)
	return r
}

func (r *httpRoute) HandlerFunc(f func(*httpRequestHandler)) *httpRoute {
	r.handlerFunc = f
	r.Route.HandlerFunc(r.handle)
	return r
}

func (r *httpRoute) handle(w http.ResponseWriter, req *http.Request) {
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
	h := &httpRequestHandler{
		requestHandler: requestHandler{
			Handler: r.Handler,
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
			debug: true,
		},
		Request: req,
		w:       httpResponseWriter{ResponseWriter: w},
	}
	defer func() {
		if err := recover(); err != nil {
			if !h.w.statusCodeSent {
				http.Error(&h.w, fmt.Sprint(err), http.StatusInternalServerError)
			}
			h.savePanic(req.RequestURI, err, debug.Stack())
		}
		h.endpointStat.report(h.w.statusCode, format.BuiltinMetricMetaAPIResponseTime.Name)
	}()
	err := h.init()
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
	} else {
		r.handlerFunc(h)
	}
	if req.Context().Err() == nil && 500 <= h.w.statusCode && h.w.statusCode < 600 {
		if err == nil {
			if h.w.handlerErr != nil {
				err = h.w.handlerErr
			} else {
				err = fmt.Errorf("status code %d", h.w.statusCode)
			}
		}
		h.savePanic(req.RequestURI, err, nil)
	}
}

func DumpInternalServerErrors(r *httpRequestHandler) {
	w := r.Response()
	if ok := r.accessInfo.insecureMode || r.accessInfo.bitAdmin; !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	r.errorsMu.RLock()
	defer r.errorsMu.RUnlock()
	for i := 0; i < len(r.errors) && r.errors[i].what != nil; i++ {
		w.Write([]byte("# \n"))
		w.Write([]byte("# " + r.errors[i].requestURI + " \n"))
		w.Write([]byte(fmt.Sprintf("# %s \n", r.errors[i].what)))
		w.Write([]byte("# " + r.errors[i].time.Format(time.RFC3339) + " \n"))
		w.Write([]byte("# debug trace\n"))
		for _, v := range r.errors[i].trace {
			w.Write([]byte(v))
			w.Write([]byte("\n"))
		}
		w.Write([]byte("# stack trace\n"))
		w.Write(r.errors[i].stack)
		w.Write([]byte("\n"))
	}
}

func DumpQueryTopMemUsage(r *httpRequestHandler) {
	w := r.Response()
	if ok := r.accessInfo.insecureMode || r.accessInfo.bitAdmin; !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	var s []queryTopMemUsage
	r.queryTopMemUsageMu.Lock()
	if r.FormValue("reset") != "" {
		r.queryTopMemUsage, s = s, r.queryTopMemUsage
	} else {
		s = make([]queryTopMemUsage, 0, len(r.queryTopMemUsage))
		s = append(s, r.queryTopMemUsage...)
	}
	r.queryTopMemUsageMu.Unlock()
	for _, v := range s {
		var protocol string
		switch v.protocol {
		case format.TagValueIDRPC:
			protocol = "RPC"
		case format.TagValueIDHTTP:
			protocol = "HTTP"
		default:
			protocol = strconv.Itoa(v.protocol)
		}
		w.Write([]byte(v.expr))
		w.Write([]byte(fmt.Sprintf(
			"\n# size=%d rows=%d cols=%d from=%d to=%d range=%d token=%s proto=%s\n\n",
			v.memUsage, v.rowCount, v.colCount, v.start, v.end, v.end-v.start, v.user, protocol)))
	}
}

func DumpQueryTopDuration(r *httpRequestHandler) {
	w := r.Response()
	if ok := r.accessInfo.insecureMode || r.accessInfo.bitAdmin; !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	var s []queryTopDuration
	r.queryTopDurationMu.Lock()
	if r.FormValue("reset") != "" {
		r.queryTopDuration, s = s, r.queryTopDuration
	} else {
		s = make([]queryTopDuration, 0, len(r.queryTopDuration))
		s = append(s, r.queryTopDuration...)
	}
	r.queryTopDurationMu.Unlock()
	for _, v := range s {
		var protocol string
		switch v.protocol {
		case format.TagValueIDRPC:
			protocol = "RPC"
		case format.TagValueIDHTTP:
			protocol = "HTTP"
		default:
			protocol = strconv.Itoa(v.protocol)
		}
		w.Write([]byte("# PromQL\n"))
		w.Write([]byte(v.expr))
		w.Write([]byte("\n# SQL\n"))
		w.Write([]byte(v.query))
		w.Write([]byte(fmt.Sprintf(
			"\n# duration=%v from=%d to=%d range=%d token=%s proto=%s\n\n",
			v.duration, v.start, v.end, v.end-v.start, v.user, protocol)))
	}
}

func (r *httpRequestHandler) Response() http.ResponseWriter {
	return &r.w
}

func (h *httpResponseWriter) Header() http.Header {
	return h.ResponseWriter.Header()
}

func (h *httpResponseWriter) Write(s []byte) (int, error) {
	return h.ResponseWriter.Write(s)
}

func (h *httpResponseWriter) WriteHeader(statusCode int) {
	h.statusCode = statusCode
	h.ResponseWriter.WriteHeader(statusCode)
	h.statusCodeSent = true
}
