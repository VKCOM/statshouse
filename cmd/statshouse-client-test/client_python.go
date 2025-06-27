package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
)

type python struct{ client }

func (*python) libMain() string {
	return "src/statshouse/_statshouse.py"
}

func (*python) gitURL() string {
	return "https://github.com/VKCOM/statshouse-py.git"
}

func (*python) testMain() string {
	return "test.py"
}

func (client *python) configure(text string, data any) error {
	if err := client.client.configure(text, data); err != nil {
		return err
	}
	return client.library.exec("pip", "install", ".")
}

func (client *python) run() error {
	cmd := exec.Command("python3", "test.py")
	cmd.Dir = client.dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("PYTHONPATH=%s", path.Join(client.library.dir, "src")))
	return cmd.Run()
}
