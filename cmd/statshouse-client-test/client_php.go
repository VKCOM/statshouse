package main

import "fmt"

type php struct{ client }

func (*php) libMain() string {
	return "src/StatsHouse.php"
}

func (*php) gitURL() string {
	return "git@github.com:VKCOM/statshouse-php.git"
}

func (*php) testMain() string {
	return "test.php"
}

func (client *php) run() error {
	return client.exec("php", "-d", fmt.Sprintf("include_path=%q", client.library.rootDir), "test.php")
}
