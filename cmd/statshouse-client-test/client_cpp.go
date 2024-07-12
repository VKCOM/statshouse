package main

import (
	_ "embed"
	"strings"
)

type cpp struct{ client }

func (*cpp) localPath() string {
	return "statshouse.hpp"
}

func (*cpp) remotePath() string {
	return "https://raw.githubusercontent.com/VKCOM/statshouse-cpp/master/statshouse.hpp"
}

func (*cpp) sourceFileName() string {
	return "main.cpp"
}

func (l *cpp) make() error {
	l.binFile = strings.TrimSuffix(l.srcFile, ".cpp")
	return l.exec("g++", "-I", l.path, "-o", l.binFile, l.srcFile)
}

type cppTransport struct{ cpp }
type cppRegistry struct{ cpp }

//go:embed template_cpp_transport.txt
var cppTransportTemplate string

func (*cppTransport) sourceFileTemplate() string {
	return cppTransportTemplate
}

//go:embed template_cpp_registry.txt
var cppRegistryTemplate string

func (*cppRegistry) sourceFileTemplate() string {
	return cppRegistryTemplate
}
