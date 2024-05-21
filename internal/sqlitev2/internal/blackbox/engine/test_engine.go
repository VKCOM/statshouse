package engine

import (
	"os"
	"os/exec"
	"path"
	"syscall"
)

type TestEngine struct {
	Prefix     string
	DBName     string
	BinaryPath string

	Writer  func()
	process *exec.Cmd
}

func (e *TestEngine) RunTest() {

}

func (e *TestEngine) StartEngine() error {
	cmd, err := run(e.BinaryPath, path.Join(e.Prefix, e.DBName), e.Prefix)
	if err != nil {
		return err
	}
	e.process = cmd
	return nil
}

func (e *TestEngine) Kill() (*os.ProcessState, error) {
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

func (e *TestEngine) Shutdown() (*os.ProcessState, error) {
	p := e.process
	err := p.Process.Signal(syscall.SIGINT)
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

func run(binaryPath, dbPath, prefix string) (*exec.Cmd, error) {
	cmd := exec.Command(binaryPath, "--db-path="+dbPath, "--binlog-prefix="+prefix)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = binaryPath
	err := cmd.Start()
	if err != nil {
		return nil, err
	}
	return cmd, nil
}
