package main

import (
	"path/filepath"
)

type java struct{ client }

func (*java) libMain() string {
	return "src/main/java/com/vk/statshouse/Client.java"
}

func (*java) gitURL() string {
	return "https://github.com/VKCOM/statshouse-java.git"
}

func (*java) testMain() string {
	return "test.java"
}

func (client *java) make() error {
	return client.exec("javac", "-cp", filepath.Join(client.library.rootDir, "src/main/java/"), "-d", client.rootDir, "test.java")
}

func (client *java) run() error {
	return client.exec("java", "test")
}
