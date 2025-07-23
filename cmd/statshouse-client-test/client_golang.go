package main

import "fmt"

type golang struct{ client }

func (*golang) libMain() string {
	return "statshouse.go"
}

func (*golang) gitURL() string {
	return "https://github.com/VKCOM/statshouse-go.git"
}

func (*golang) testMain() string {
	return "test.go"
}

func (client *golang) configure(text string, data any) error {
	if err := client.client.configure(text, data); err != nil {
		return err
	}
	if err := client.exec("go", "mod", "init", "main"); err != nil {
		return err
	}
	if err := client.exec("go", "mod", "edit",
		fmt.Sprintf("-replace=github.com/VKCOM/statshouse-go=%s", client.library.rootDir)); err != nil {
		return err
	}
	return client.exec("go", "get")
}

func (client *golang) run() error {
	return client.exec("go", "run", "test.go")
}
