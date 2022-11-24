// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package srvfunc

import (
	"io"
	"os"
	"runtime/pprof"
)

type (
	cpuprofiler struct {
	}

	memprofiler struct {
		fd io.WriteCloser
	}

	gorprofiler struct {
		fd io.WriteCloser
	}
)

// MakeCPUProfile инициализирует запись cpu профиля в файл
// Файл наполняется все время работы приложения
func MakeCPUProfile(path string) (io.Closer, error) {
	if f, err := os.Create(path); err != nil {
		return nil, err
	} else if err := pprof.StartCPUProfile(f); err != nil {
		return nil, err
	} else {
		return &cpuprofiler{}, nil
	}
}

// MakeMemProfile открывает файл для записи mem профиля
// Реальная запись в файл происходит при закрытии профилировщика
func MakeMemProfile(path string) (io.Closer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &memprofiler{fd: f}, nil
}

// MakeGorProfile открывает файл для записи статистике по горутинам
// Реальная запись в файл происходит при закрытии профилировщика
func MakeGorProfile(path string) (io.Closer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &gorprofiler{fd: f}, nil
}

func (cp *cpuprofiler) Close() error {
	pprof.StopCPUProfile()
	return nil
}

func (mp *memprofiler) Close() error {
	defer mp.fd.Close()
	return pprof.WriteHeapProfile(mp.fd)
}

func (gp *gorprofiler) Close() error {
	defer gp.fd.Close()
	return pprof.Lookup(`goroutine`).WriteTo(gp.fd, 1)
}
