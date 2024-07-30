package main

import (
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"text/template"
	"unsafe"
)

type lib interface {
	libMain() string
	testMain() string
	gitURL() string
	gitBranch() string
	configure(string, any) error
	make() error
	run() error
}

type library struct {
	shell
	rootDir string
	factory func(argv, string) lib
}

type client struct {
	shell
	*library
	impl    lib
	args    argv
	rootDir string
	srcFile string
	binFile string
}

type shell struct {
	dir string
}

func (*library) gitBranch() string {
	return ""
}

func (client *client) configure(text string, data any) error {
	t, err := template.New("").Parse(text)
	if err != nil {
		return err
	}
	client.srcFile = filepath.Join(client.rootDir, client.impl.testMain())
	if err = os.MkdirAll(filepath.Dir(client.srcFile), os.ModePerm); err != nil {
		return err
	}
	srcFile, err := os.Create(client.srcFile)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	if err := t.Execute(srcFile, data); err != nil {
		return err
	}
	if client.args.viewCode {
		client.exec("code", client.srcFile)
	}
	return nil
}

func (*client) make() error {
	return nil // nop, works for interpreted languages
}

func (client *client) run() error {
	return client.exec(client.binFile) // works for compiled languages
}

func (sh *shell) exec(args ...string) error {
	log.Println("$ cd", sh.dir)
	log.Printf("$ %s\n", strings.Join(args, " "))
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = sh.dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func loadLibraryFromWD() (lib *library, cancel func(), err error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, nil, err
	}
	if lib, cancel, err = loadLibrary[cpp](wd); lib != nil {
		return lib, cancel, err
	}
	if lib, cancel, err = loadLibrary[golang](wd); lib != nil {
		return lib, cancel, err
	}
	if lib, cancel, err = loadLibrary[java](wd); lib != nil {
		return lib, cancel, err
	}
	if lib, cancel, err = loadLibrary[php](wd); lib != nil {
		return lib, cancel, err
	}
	if lib, cancel, err = loadLibrary[python](wd); lib != nil {
		return lib, cancel, err
	}
	if lib, cancel, err = loadLibrary[rust](wd); lib != nil {
		return lib, cancel, err
	}
	return nil, func() {}, nil
}

func loadLibrary[T any](path string) (res *library, cancel func(), err error) {
	var ok bool
	defer func() {
		if !ok && cancel != nil {
			cancel()
		}
	}()
	var t T // nil
	l := any(&t).(lib)
	if path == "" {
		res = &library{}
		res.rootDir, err = os.MkdirTemp("", "")
		if err != nil {
			return nil, nil, err
		}
		res.shell.dir = res.rootDir
		cancel = func() {
			log.Println("$ rm -rf", res.rootDir)
			os.RemoveAll(res.rootDir)
		}
		if branchName := l.gitBranch(); branchName == "" {
			err = res.exec("git", "clone", "--depth=1", "--no-tags", l.gitURL(), res.rootDir)
		} else {
			err = res.exec("git", "clone", "--depth=1", "--no-tags", "-b", branchName, l.gitURL(), res.rootDir)
		}
		if err != nil {
			return nil, nil, err
		}
	} else if _, err := os.Stat(filepath.Join(path, l.libMain())); err == nil {
		res = &library{shell: shell{dir: path}, rootDir: path}
		cancel = func() {}
	} else {
		return nil, nil, nil
	}
	res.factory = func(args argv, dir string) lib {
		var t T
		f0 := reflect.ValueOf(&t).Elem().Field(0) // assumed every client has first field of type "client"
		client := reflect.NewAt(f0.Type(), unsafe.Pointer(f0.UnsafeAddr())).Interface().(*client)
		client.args = args
		client.rootDir = dir
		client.library = res
		client.impl = any(&t).(lib)
		client.shell.dir = dir
		return client.impl
	}
	ok = true
	return res, cancel, nil
}

func newClient(lib *library, args argv) (_ lib, cancel func(), err error) {
	path, err := os.MkdirTemp("", "")
	if err != nil {
		return nil, nil, err
	}
	var ok bool
	if args.keepTemp {
		cancel = func() {} // nop
	} else {
		cancel = func() {
			log.Println("$ rm -rf", path)
			os.RemoveAll(path)
		}
	}
	defer func() {
		if !ok {
			cancel()
		}
	}()
	res := lib.factory(args, path)
	ok = true
	return res, cancel, nil
}

func testTemplates(lib *library) map[string]string {
	glob := filepath.Join(lib.rootDir, "test_template*.txt")
	matches, err := filepath.Glob(glob)
	if err != nil {
		log.Fatal(err)
	}
	res := make(map[string]string, len(matches))
	for _, path := range matches {
		file, err := os.Open(path)
		if err != nil {
			log.Fatal(err)
		}
		var sb strings.Builder
		if _, err = io.Copy(&sb, file); err != nil {
			log.Fatal(err)
		}
		res[filepath.Base(path)] = sb.String()
	}
	return res
}

func runClient(args argv, lib *library, text string, data any) error {
	client, cancel, err := newClient(lib, args)
	if err != nil {
		return err
	}
	defer cancel()
	if err := client.configure(text, data); err != nil {
		return err
	}
	if err := client.make(); err != nil {
		return err
	}
	return client.run()
}
