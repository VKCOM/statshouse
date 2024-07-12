package main

import (
	_ "embed"
	"fmt"
)

type golang struct{ client }

func (*golang) localPath() string {
	return "statshouse.go"
}

//go:embed template_golang.txt
var golangTemplate string

func (*golang) sourceFileTemplate() string {
	return golangTemplate
}

func (*golang) sourceFileName() string {
	return "main.go"
}

func (l *golang) configure(d any) error {
	if err := l.client.configure(d); err != nil {
		return err
	}
	if err := l.exec("go", "mod", "init", "main"); err != nil {
		return err
	}
	if l.path != "" {
		if err := l.exec("go", "mod", "edit", fmt.Sprintf("-replace=github.com/vkcom/statshouse-go=%s", l.path)); err != nil {
			return err
		}
	}
	return l.exec("go", "get")
}

func (l *golang) run() error {
	return l.exec("go", "run", "main.go")
}
