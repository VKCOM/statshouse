package main

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/format"
)

type panicHandler struct {
	h http.Handler
}

func (p *panicHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			statshouse.Metric(format.BuiltinMetricNameStatsHouseErrors, statshouse.Tags{1: strconv.FormatInt(format.TagValueIDAPIPanicError, 10)}).StringTop(fmt.Sprintf("%v", r))
			panic(r)
		}
	}()
	p.h.ServeHTTP(writer, request)

}
