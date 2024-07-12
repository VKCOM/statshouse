package main

import (
	_ "embed"
	"path/filepath"
)

type java struct{ client }

func (*java) localPath() string {
	return "src/main/java/com/vk/statshouse/Client.java"
}

func (*java) remotePath() string {
	return "git@github.com:VKCOM/statshouse-java.git"
}

//go:embed template_java.txt
var javaTemplate string

func (*java) sourceFileTemplate() string {
	return javaTemplate
}

func (*java) sourceFileName() string {
	return "test.java"
}

func (c *java) make() error {
	return c.exec("javac", "-cp", filepath.Join(c.path, "src/main/java/"), "-d", c.temp, "test.java")
}

func (c *java) run() error {
	return c.exec("java", "test")
}
