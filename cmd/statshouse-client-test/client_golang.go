package main

import "fmt"

type golang struct{ client }

func (*golang) localPath() string {
	return "statshouse.go"
}

func (*golang) sourceFileName() string {
	return "main.go"
}

func (l *golang) configure(text string, data any) error {
	if err := l.client.configure(text, data); err != nil {
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
