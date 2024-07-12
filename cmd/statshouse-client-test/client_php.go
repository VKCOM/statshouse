package main

import (
	_ "embed"
	"fmt"
)

type php struct{ client }

func (*php) localPath() string {
	return "src/StatsHouse.php"
}

func (*php) remotePath() string {
	return "https://raw.githubusercontent.com/VKCOM/statshouse-php/master/src/StatsHouse.php"
}

//go:embed template_php.txt
var phpTemplate string

func (*php) sourceFileTemplate() string {
	return phpTemplate
}

func (*php) sourceFileName() string {
	return "main.php"
}

func (l *php) run() error {
	return l.exec("php", "-d", fmt.Sprintf("include_path=%q", l.path), "main.php")
}
