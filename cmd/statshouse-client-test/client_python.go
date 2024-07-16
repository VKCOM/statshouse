package main

type python struct{ client }

func (*python) localPath() string {
	return "src/statshouse/_statshouse.py"
}

func (*python) remotePath() string {
	return "git@github.com:VKCOM/statshouse-py.git"
}

func (*python) sourceFileName() string {
	return "main.py"
}

func (l *python) run() error {
	return l.exec("python3", "main.py")
}
