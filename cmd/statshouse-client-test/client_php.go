package main

import "fmt"

type php struct{ client }

func (*php) localPath() string {
	return "src/StatsHouse.php"
}

func (*php) remotePath() string {
	return "git@github.com:VKCOM/statshouse-php.git"
}

func (*php) sourceFileName() string {
	return "main.php"
}

func (l *php) run() error {
	return l.exec("php", "-d", fmt.Sprintf("include_path=%q", l.path), "main.php")
}
