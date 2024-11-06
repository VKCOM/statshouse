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
	debug        bool
	forceVersion string
	versionDice  func() string
}

type httpRequestHandler struct {
	requestHandler
	*http.Request
	w httpResponseWriter
}

type httpResponseWriter struct {
	http.ResponseWriter
	statusCode     int
	statusCodeSent bool
}

type error500 struct {
	time  time.Time
	what  any
	stack []byte
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
			h.savePanic(err, debug.Stack())
		}
		h.endpointStat.report(h.w.statusCode, format.BuiltinMetricNameAPIResponseTime)
	}()
	if err := h.init(); err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	r.handlerFunc(h)
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
		w.Write([]byte(fmt.Sprintf("# %s \n", r.errors[i].what)))
		w.Write([]byte("# " + r.errors[i].time.Format(time.RFC3339) + " \n"))
		w.Write([]byte("# \n"))
		w.Write(r.errors[i].stack)
		w.Write([]byte("\n"))
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
