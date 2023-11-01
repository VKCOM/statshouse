package blackbox

import (
	"fmt"
	"os"
	"os/exec"
)

type engine struct {
	prefix  string
	db      string
	process *exec.Cmd
}

func runEngine(dbPath, prefix string) (*engine, error) {
	cmd, err := run(dbPath, prefix)
	if err != nil {
		return nil, err
	}
	return &engine{
		db:      dbPath,
		prefix:  prefix,
		process: cmd,
	}, nil
}

func createBinlog(prefix, createBinlog string) error {
	cmd := exec.Command("./kv_engine", "--binlog-prefix="+prefix, "--create-binlog="+createBinlog)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = "/Users/e.martyn/Documents/vk/statshouse/internal/sqlitev2/internal/blackbox/kv_engine"
	return cmd.Run()
}

func run(dbPath, prefix string) (*exec.Cmd, error) {
	cmd := exec.Command("./kv_engine", "--db-path="+dbPath, "--binlog-prefix="+prefix)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = "/Users/e.martyn/Documents/vk/statshouse/internal/sqlitev2/internal/blackbox/kv_engine"
	fmt.Println(cmd.Path)
	err := cmd.Start()
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (e *engine) kill() (*os.ProcessState, error) {
	p := e.process
	err := p.Process.Kill()
	if err != nil {
		return nil, err
	}
	e.process = nil
	state, err := p.Process.Wait()
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (e *engine) restart(dbPath string) error {
	if e.process != nil {
		return fmt.Errorf("failed to restart. Kill engine")
	}
	cmd, err := run(dbPath, e.prefix)
	if err != nil {
		return err
	}
	e.db = dbPath
	e.process = cmd
	return nil
}
