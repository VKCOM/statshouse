// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"os/user"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cloudflare/tableflip"
	"github.com/spf13/pflag"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
	"github.com/vkcom/statshouse/internal/vkgo/statlogs"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlengine"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/metadata"
)

var argv struct {
	dbPath  string
	rpcPort int

	shAddr           string
	shEnv            string
	pidFile          string
	rpcCryptoKeyPath string

	createBinlog string
	binlogPrefix string

	user       string
	group      string
	replacePid int

	maxBudget    int64
	stepSec      uint32
	budgetBonus  int64
	globalBudget int64

	version    bool
	help       bool
	secureMode bool
}

type Logger struct{}

func (*Logger) Tracef(format string, args ...interface{}) { log.Printf(format, args...) }
func (*Logger) Debugf(format string, args ...interface{}) { log.Printf(format, args...) }
func (*Logger) Infof(format string, args ...interface{})  { log.Printf(format, args...) }
func (*Logger) Warnf(format string, args ...interface{})  { log.Printf(format, args...) }
func (*Logger) Errorf(format string, args ...interface{}) { log.Printf(format, args...) }

const upgradeTimeout = 60 * time.Second

func parseArgs() {
	pflag.StringVar(&argv.dbPath, "db-path", "", "path to sqlite storage file")
	pflag.StringVar(&argv.pidFile, "pid-file", "", "path to PID file")
	pflag.IntVar(&argv.replacePid, "replace-pid", 0, "specified pid will be killed by SIGTERM after our init")

	pflag.StringVar(&argv.rpcCryptoKeyPath, "rpc-crypto-path", "", "path to RPC crypto key. if empty try to use stdin")

	pflag.StringVar(&argv.shAddr, "statshouse-addr", statlogs.DefaultStatsHouseAddr, "address of StatsHouse UDP socket")
	pflag.StringVar(&argv.shEnv, "statshouse-env", "dev", "fill key0/environment with this value in StatHouse statistics")
	pflag.BoolVar(&argv.secureMode, "secure", false, "if set, fail if can't read rpc crypto key from rpc-crypto-path or from stdin")

	pflag.StringVar(&argv.createBinlog, "create-binlog", "", "creates empty binlog for engine. <arg> should be <engine_id>,<cluster_size>")
	pflag.StringVar(&argv.binlogPrefix, "binlog-prefix", "", "where to store binlog data")

	var logFile string
	pflag.StringVarP(&logFile, "log-file", "l", "", "legacy. This flag mustn't be used")
	pflag.StringVarP(&argv.user, "user", "u", "", "legacy. This flag mustn't be used")
	pflag.StringVarP(&argv.group, "group", "g", "", "legacy. This flag mustn't be used")
	pflag.IntVarP(&argv.rpcPort, "port", "p", 2442, "RPC server port")
	pflag.BoolVarP(&argv.help, "help", "h", false, "print usage instructions and exit")
	pflag.BoolVar(&argv.version, "version", false, "show version information and exit")

	pflag.Int64Var(&argv.maxBudget, "max-budget", metadata.MaxBudget, "maximum number of mappings that a metric can create")
	pflag.Uint32Var(&argv.stepSec, "step-sec", metadata.StepSec, "every step-sec metric will receive budget-bonus mappings to budget")
	pflag.Int64Var(&argv.budgetBonus, "budget-bonus", metadata.BudgetBonus, "every step-sec seconds metric will receive budget-bonus mappings to budget")
	pflag.Int64Var(&argv.budgetBonus, "global-budget", metadata.GlobalBudget, "create mapping budget. After spent this budget meta will use step system")

	pflag.Parse()
}

const binlogMagic = 0xdf1273ab

func lookupGroup(groupname string) (int, error) {
	group, err := user.LookupGroup(groupname)
	if err != nil {
		return 0, fmt.Errorf(`could not get group %s: %w`, groupname, err)
	} else if gid, err := strconv.ParseUint(group.Gid, 10, 32); err != nil {
		return 0, fmt.Errorf(`could not parse group id %s: %w`, group.Gid, err)
	} else {
		return int(gid), nil
	}
}

func lookupUser(username string) (int, error) {
	_user, err := user.Lookup(username)
	if err != nil {
		return 0, fmt.Errorf(`could not get user %s: %w`, username, err)
	} else if uid, err := strconv.ParseUint(_user.Uid, 10, 32); err != nil {
		return 0, fmt.Errorf(`could not parse user id %s: %w`, _user.Uid, err)
	} else {
		return int(uid), nil
	}
}

func setupUser(user, group string) {
	if os.Getuid() != 0 {
		return
	}

	if group != `` {
		if gid, err := lookupGroup(group); err != nil {
			log.Fatal(fmt.Errorf(`could not get group %s: %w`, group, err))
		} else if err := syscall.Setgid(gid); err != nil {
			log.Fatal(fmt.Errorf(`could not change group to %s: %w`, group, err))
		}
	}

	if user != `` {
		if uid, err := lookupUser(user); err != nil {
			log.Fatal(fmt.Errorf(`could not get user %s: %w`, user, err))
		} else if err := syscall.Setuid(uid); err != nil {
			log.Fatal(fmt.Errorf(`could not change user to %s: %w`, user, err))
		}
	}
}

func processCreateBinlogParam(createBinlog string) (engineID uint64, clusterSize uint64, err error) {
	if createBinlog == `` {
		return 0, 0, nil
	}
	chunks := strings.Split(createBinlog, `,`)
	if len(chunks) != 2 {
		return 0, 0, fmt.Errorf(`expect format <engine_id>,<cluster_size> got %s`, createBinlog)
	}
	engineID, err = strconv.ParseUint(chunks[0], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf(`could not parse engine_id: %w`, err)
	}
	clusterSize, err = strconv.ParseUint(chunks[1], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf(`could not parse cluster_size: %w`, err)
	}
	return engineID, clusterSize, nil
}

func main() {
	log.SetPrefix("[statshouse-metadata] ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lmsgprefix)
	parseArgs()
	if argv.help {
		pflag.Usage()
		return
	}
	if argv.version {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", build.Info())
		return
	}
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {

	setupUser(argv.user, argv.group)
	if argv.createBinlog != "" {
		engineID, clusterSize, err := processCreateBinlogParam(argv.createBinlog)
		if err != nil {
			return err
		}
		_, err = fsbinlog.CreateEmptyFsBinlog(binlog.Options{
			PrefixPath:        argv.binlogPrefix,
			Magic:             binlogMagic,
			EngineIDInCluster: uint(engineID),
			ClusterSize:       uint(clusterSize),
		})
		if err != nil {
			return err
		}
		return nil
	}
	var rpcCryptoKeys []string
	log.Println("[debug] starting read crypto key")
	if argv.rpcCryptoKeyPath != "" {
		cryptoKey, err := os.ReadFile(argv.rpcCryptoKeyPath)
		if err != nil {
			return fmt.Errorf("could not read RPC crypto key file %q: %v", argv.rpcCryptoKeyPath, err)
		}
		rpcCryptoKeys = append(rpcCryptoKeys, string(cryptoKey))
	} else if argv.secureMode {
		keyBytes := make([]byte, 4096)
		n, err := os.Stdin.Read(keyBytes)
		if err == io.EOF {
			log.Fatal("rpc key shouldn't be empty")
		}
		if err != nil {
			return err
		}
		if n == 0 {
			log.Fatal("rpc key shouldn't be empty")
		}
		keyBytes = keyBytes[:n]
		rpcCryptoKeys = append(rpcCryptoKeys, string(keyBytes))
	}
	err := replacePid(argv.replacePid)
	if err != nil {
		return fmt.Errorf("can't kill old process: %w", err)
	}
	bl, err := fsbinlog.NewFsBinlog(&Logger{}, binlog.Options{
		PrefixPath: argv.binlogPrefix,
		Magic:      binlogMagic,
	})

	if err != nil {
		return fmt.Errorf("can't init binlog: %w", err)
	}

	if argv.dbPath == "" {
		return fmt.Errorf("--db-path must not be empty'")
	}

	tf, err := tableflip.New(tableflip.Options{
		PIDFile:        argv.pidFile,
		UpgradeTimeout: upgradeTimeout,
	})
	if err != nil {
		return err
	}
	defer tf.Stop()
	rpcLn, err := tf.Listen("tcp4", fmt.Sprintf(":%d", argv.rpcPort))
	if err != nil {
		return err
	}
	if err := tf.Ready(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	err = tf.WaitForParent(ctx)
	cancel()
	if err != nil {
		return err
	}
	host := srvfunc.HostnameForStatshouse()
	log.Println("[debug] opening db and reread binlog")
	db, err := metadata.OpenDB(argv.dbPath, metadata.Options{
		Host:         host,
		MaxBudget:    argv.maxBudget,
		StepSec:      argv.stepSec,
		BudgetBonus:  argv.budgetBonus,
		GlobalBudget: argv.globalBudget,
	}, bl)
	if err != nil {
		return fmt.Errorf("db-path: %s, failed to open db: %w", argv.dbPath, err)
	}
	defer func() {
		err := bl.Shutdown()
		if err != nil {
			log.Printf("[error] %v", err)
		}
	}()
	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("[error] %v", err)
		}
	}()
	statlogs.Configure(log.Printf, argv.shAddr, argv.shEnv)
	defer statlogs.Close()

	proxy := metadata.ProxyHandler{Host: host}
	handler := metadata.NewHandler(db, host, log.Printf)
	h := &tlmetadata.Handler{
		RawGetMapping:       proxy.HandleProxy("getMapping", handler.RawGetMappingByValue),
		RawPutMapping:       proxy.HandleProxy("putMapping", handler.RawPutMapping),
		RawGetInvertMapping: proxy.HandleProxy("getMappingByID", handler.RawGetMappingByID),
		RawResetFlood:       proxy.HandleProxy("resetFlood", handler.RawResetFlood),

		RawGetJournalnew:       proxy.HandleProxy("getJournal", handler.RawGetJournal),
		RawEditEntitynew:       proxy.HandleProxy("editEntity", handler.RawEditEntity),
		PutTagMappingBootstrap: handler.PutTagMappingBootstrap,
		GetTagMappingBootstrap: handler.GetTagMappingBootstrap,
		ResetFlood2:            handler.ResetFlood2,
	}

	engineRPCHandler := metadata.NewEngineRpcHandler(argv.binlogPrefix, db)
	engineHandler := &tlengine.Handler{
		SendSignal:        engineRPCHandler.SendSignal,
		GetReindexStatus:  engineRPCHandler.GetReindexStatus,
		GetBinlogPrefixes: engineRPCHandler.GetBinlogPrefixes,
	}

	server := &rpc.Server{
		Handler:             rpc.ChainHandler(h.Handle, engineHandler.Handle),
		Logf:                log.Printf,
		TrustedSubnetGroups: build.TrustedSubnetGroups(),
		CryptoKeys:          rpcCryptoKeys,
		MaxWorkers:          5000,
		ResponseBufSize:     1024,
		ResponseMemEstimate: 1024,
	}
	go func() {
		err = server.Serve(rpcLn)
		if err != rpc.ErrServerClosed {
			log.Println(err)
		}
	}()
	log.Println("[debug] listen and start serving:", rpcLn.Addr())
	log.Println("[debug] pid:", os.Getpid())
	const indexSignal = syscall.Signal(64)
	signal.Notify(engineRPCHandler.SignalCh, syscall.SIGINT, syscall.SIGTERM, indexSignal)
	finishCh := make(chan struct{}, 1)
	go func() {
	loop:
		for sig := range engineRPCHandler.SignalCh {
			switch sig {
			case indexSignal:
				log.Println("[debug] starting backup")
				err := engineRPCHandler.Backup(context.Background(), argv.binlogPrefix)
				if err != nil {
					log.Printf("[error] %v", err)
				} else {
					log.Println("[debug] backup was done to")
				}

			case syscall.SIGINT, syscall.SIGTERM:
				log.Printf("[debug] got %v, exiting...", sig)
				finishCh <- struct{}{}
				break loop
			}
		}
	}()
	select {
	case <-tf.Exit():
	case <-finishCh:
	}
	time.AfterFunc(30*time.Second, func() {
		log.Println("[error] graceful shutdown timed out")
		os.Exit(1)
	})
	err = server.Close()
	if err != nil {
		return err
	}
	return nil
}

func replacePid(pid int) error {
	if pid == 0 {
		return nil
	}

	const killSig = syscall.SIGTERM
	log.Println("[debug] sending signal to replace pid", killSig)

	if _, _, errno := syscall.RawSyscall(syscall.SYS_KILL, uintptr(pid), uintptr(killSig), 0); errno != 0 {
		if errno != syscall.ESRCH {
			return fmt.Errorf("can't send %q to pid %d: %w", killSig, pid, errno)
		}
		return fmt.Errorf("there is no process %d", pid)
	}

	fromTs := time.Now()
	killTicker := time.NewTicker(10 * time.Millisecond)
	logTicker := time.NewTicker(500 * time.Millisecond)
	timeout := time.After(120 * time.Second) // #define DEFAULT_KILL_TIMEOUT_US 12e7

	defer func() {
		killTicker.Stop()
		logTicker.Stop()
	}()

loop:
	for {
		select {
		case <-timeout:
			return fmt.Errorf("waiting previous instance for terminating too long")
		case <-killTicker.C:
			if err := syscall.Kill(pid, 0); err != nil {
				log.Println(fmt.Errorf("hot swapped by killing previous instance (check for previous instance): %w", err))
				break loop
			}
		case <-logTicker.C:
			log.Println("[debug] process is not terminated yet after", time.Since(fromTs).String())
		}
	}
	log.Println("[debug] previous process successfully hot swapped", time.Since(fromTs).String())

	return nil
}
