package main

import (
	_ "embed"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"text/template"
)

type lib interface {
	// library info
	localPath() string
	remotePath() string
	sourceFileName() string
	// client builder
	init(lib, argv, string)
	configure(string, any) error
	make() error
	run() error
	cleanup()
}

type client struct {
	lib
	args    argv
	path    string
	temp    string
	srcFile string
	binFile string
}

func (c *client) init(lib lib, args argv, path string) {
	c.lib = lib
	c.args = args
	c.path = path
}

func (c *client) remotePath() string {
	return "" // works for golang ("go get" knows where to get the code)
}

func (c *client) configure(text string, data any) error {
	var err error
	c.temp, err = os.MkdirTemp("", typeName(c.lib))
	if err != nil {
		return nil
	}
	log.Println("$ cd", c.temp)
	if c.path == "" {
		if remotePath := c.lib.remotePath(); remotePath != "" {
			if err := c.clone(remotePath); err != nil {
				return err
			}
		}
	}
	t, err := template.New(typeName(c.lib)).Parse(text)
	if err != nil {
		return err
	}
	c.srcFile = filepath.Join(c.temp, c.sourceFileName())
	if err = os.MkdirAll(filepath.Dir(c.srcFile), os.ModePerm); err != nil {
		return err
	}
	srcFile, err := os.Create(c.srcFile)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	if err := t.Execute(srcFile, data); err != nil {
		return err
	}
	if c.args.viewCode {
		c.exec("code", c.srcFile)
	}
	return nil
}

func (c *client) make() error {
	return nil // nop, works for interpreted languages
}

func (c *client) run() error {
	return c.exec(c.binFile) // works for compiled languages
}

func (c *client) cleanup() {
	if c.args.keepTemp || c.temp == "" {
		return
	}
	os.RemoveAll(c.temp)
}

func (c *client) clone(url string) error {
	if strings.HasSuffix(url, ".git") {
		return c.exec("git", "clone", "--depth=1", "--no-tags", url, ".")
	} else {
		log.Printf("download %s\n", url)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		path := path.Join(c.temp, c.lib.localPath())
		if err = os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
			return err
		}
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(file, resp.Body)
		return err
	}
}

func (c *client) exec(args ...string) error {
	log.Printf("$ %s\n", strings.Join(args, " "))
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = c.temp
	return cmd.Run()
}

func search(l lib) string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	_, err = os.Stat(filepath.Join(wd, l.localPath()))
	if err != nil {
		return ""
	}
	return wd
}

func runClient(client lib, text string, data any) error {
	defer client.cleanup()
	if err := client.configure(text, data); err != nil {
		return err
	}
	if err := client.make(); err != nil {
		return err
	}
	return client.run()
}

func typeName(l lib) string {
	return reflect.ValueOf(l).Type().Elem().Name()
}
