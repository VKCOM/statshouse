package main

import "strings"

type rust struct{ client }

func (*rust) localPath() string {
	return "statshouse/src/lib.rs"
}

func (*rust) remotePath() string {
	return "git@github.com:VKCOM/statshouse-py.git"
}

func (*rust) sourceFileName() string {
	return "main.rs"
}

func (l *rust) make() error {
	l.binFile = strings.TrimSuffix(l.srcFile, ".rs")
	return l.exec("rustc", "-o", l.binFile, l.srcFile)
}
