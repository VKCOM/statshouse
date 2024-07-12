package main

import (
	_ "embed"
	"strings"
)

type rust struct{ client }

func (*rust) localPath() string {
	return "statshouse/src/lib.rs"
}

func (*rust) remotePath() string {
	return "https://raw.githubusercontent.com/VKCOM/statshouse-rs/malpinskiy/dev/statshouse/src/lib.rs"
}

//go:embed template_rust.txt
var rustTemplate string

func (*rust) sourceFileTemplate() string {
	return rustTemplate
}

func (*rust) sourceFileName() string {
	return "main.rs"
}

func (l *rust) make() error {
	l.binFile = strings.TrimSuffix(l.srcFile, ".rs")
	return l.exec("rustc", "-o", l.binFile, l.srcFile)
}
