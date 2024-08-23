package env

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

type Loader struct {
	mx  sync.RWMutex
	env Env
}

type Env struct {
	Hostname string `yaml:"hostname"`

	EnvT    string `yaml:"env"`
	Group   string `yaml:"group"`
	DC      string `yaml:"dc"`
	Region  string `yaml:"region"`
	Cluster string `yaml:"cluster"`
	Owner   string `yaml:"owner"`
}

type Environment struct {
	Name       string
	Service    string
	Cluster    string
	DataCenter string
}

const maxEnvFileSize = 1024 * 1024 * 16

func (l *Loader) Load() Env {
	l.mx.RLock()
	defer l.mx.RUnlock()
	return l.env
}

func getStamps(path string) (time.Time, int64) {
	fi, err := os.Stat(path)
	if err != nil {
		return time.Time{}, 0
	}
	return fi.ModTime(), fi.Size()
}

// if error return empty Env
func ListenEnvFile(filePath string) (*Loader, error) {
	l := &Loader{}
	if filePath == "" {
		return l, nil
	}

	lastTime, lastSize := getStamps(filePath)
	l.env, _ = ReadEnvFileWithLog(filePath)

	go func() {
		for {
			time.Sleep(time.Second)
			t, s := getStamps(filePath)
			if lastTime == t && lastSize == s {
				continue
			}
			lastTime, lastSize = t, s
			envYaml, err := ReadEnvFileWithLog(filePath)
			if err != nil {
				continue
			}
			l.mx.Lock()
			l.env = envYaml
			l.mx.Unlock()
		}
	}()
	return l, nil
}

func ReadEnvFileWithLog(filePath string) (Env, error) {
	env, err := ReadEnvFile(filePath)
	if err != nil {
		log.Printf("env reading error: %v", err)
	} else {
		log.Printf("read env file: %+v", env)
	}
	return env, err
}

func ReadEnvFile(filePath string) (Env, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return Env{}, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()
	limitReader := io.LimitReader(f, maxEnvFileSize)
	data, err := io.ReadAll(limitReader)
	if err != nil {
		return Env{}, fmt.Errorf("failed to read env file: %w", err)
	}
	env := Env{}
	err = yaml.Unmarshal(data, &env)
	if err != nil {
		return Env{}, fmt.Errorf("failed to unmarshal env: %w", err)
	}
	return env, nil
}

func ReadEnvironment(service string) Environment {
	res := Environment{
		Service: service,
	}
	envFilePath := "/etc/statshouse_env.yml"
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "env-file-path":
			envFilePath = f.Value.String()
		case "statshouse-env":
			res.Name = f.Value.String()
		}
	})
	// environment file is optional, command line takes precedence over it
	if v, err := ReadEnvFile(envFilePath); err == nil {
		if res.Name == "" {
			res.Name = v.EnvT
		}
		res.Cluster = v.Cluster
		res.DataCenter = v.DC
	}
	return res
}
