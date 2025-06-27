package main

import (
	"fmt"
	"io/fs"
	"path/filepath"
)

type rust struct{ client }

func (*rust) libMain() string {
	return "statshouse/src/lib.rs"
}

func (*rust) testMain() string {
	return "test.rs"
}

func (*rust) gitURL() string {
	return "https://github.com/VKCOM/statshouse-rs.git"
}

func (client *rust) make() error {
	if err := client.library.exec("cargo", "build"); err != nil {
		return err
	}
	var libraryPath string
	filepath.Walk(
		filepath.Join(client.library.rootDir, "target"),
		func(path string, info fs.FileInfo, err error) error {
			if !info.IsDir() && info.Name() == "libstatshouse.rlib" {
				libraryPath = path
				return fmt.Errorf("found")
			}
			return nil // continue search
		})
	client.binFile = filepath.Join(client.dir, "test")
	return client.exec("rustc", "--extern", fmt.Sprintf("statshouse=%s", libraryPath), "-o", client.binFile, "test.rs")
}
