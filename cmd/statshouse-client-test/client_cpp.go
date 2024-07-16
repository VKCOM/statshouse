package main

import "strings"

type cpp struct{ client }

func (*cpp) localPath() string {
	return "statshouse.hpp"
}

func (*cpp) remotePath() string {
	return "git@github.com:VKCOM/statshouse-cpp.git"
}

func (*cpp) sourceFileName() string {
	return "main.cpp"
}

func (l *cpp) make() error {
	l.binFile = strings.TrimSuffix(l.srcFile, ".cpp")
	if l.path != "" {
		return l.exec("g++", "-I", l.path, "-o", l.binFile, l.srcFile)
	} else {
		return l.exec("g++", "-o", l.binFile, l.srcFile)
	}
}
