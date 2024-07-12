package main

import (
	_ "embed"
)

type python struct{ client }

func (*python) localPath() string {
	return "src/statshouse/_statshouse.py"
}

func (*python) remotePath() string {
	return "git@github.com:VKCOM/statshouse-py.git"
}

//go:embed template_python.txt
var pythonTemplate string

func (*python) sourceFileTemplate() string {
	return pythonTemplate
}

func (*python) sourceFileName() string {
	return "main.py"
}

func (l *python) run() error {
	return l.exec("python3", "main.py")
}
