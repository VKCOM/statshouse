package main

import (
	"net/http"

	"github.com/vkcom/statshouse/internal/format"
)

type panicHandler struct {
	h http.Handler
}

func (p *panicHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			format.ReportAPIPanic(r)
			panic(r)
		}
	}()
	p.h.ServeHTTP(writer, request)
}
