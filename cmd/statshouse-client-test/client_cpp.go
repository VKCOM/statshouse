package main

import "path/filepath"

type cpp struct{ client }

func (*cpp) libMain() string {
	return "statshouse.hpp"
}

func (*cpp) testMain() string {
	return "test.cpp"
}

func (*cpp) gitURL() string {
	return "git@github.com:VKCOM/statshouse-cpp.git"
}

func (client *cpp) make() error {
	client.binFile = filepath.Join(client.shell.dir, "test")
	return client.exec("g++", "-I", client.library.rootDir, "-o", client.binFile, client.srcFile)
}
