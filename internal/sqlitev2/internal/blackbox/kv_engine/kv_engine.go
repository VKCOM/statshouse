package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/pflag"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlkv_engine"
	"github.com/vkcom/statshouse/internal/sqlitev2"
	"github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

var argv struct {
	dbPath  string
	rpcPort int

	createBinlog string
	binlogPrefix string
}

func parseArgs() {
	pflag.StringVar(&argv.dbPath, "db-path", "", "path to sqlite storage file")
	pflag.StringVar(&argv.createBinlog, "create-binlog", "", "creates empty binlog for engine. <arg> should be <engine_id>,<cluster_size>")
	pflag.StringVar(&argv.binlogPrefix, "binlog-prefix", "", "where to store binlog data")
	pflag.IntVarP(&argv.rpcPort, "port", "p", 2442, "RPC server port")
	pflag.Parse()
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

const BinlogMagic = 123

func main() {
	parseArgs()
	if argv.createBinlog != "" {
		engineID, clusterSize, err := processCreateBinlogParam(argv.createBinlog)
		if err != nil {
			panic(err)
		}
		_, err = fsbinlog.CreateEmptyFsBinlog(binlog.Options{
			PrefixPath:        argv.binlogPrefix,
			Magic:             BinlogMagic,
			EngineIDInCluster: uint(engineID),
			ClusterSize:       uint(clusterSize),
		})
		if err != nil {
			panic(err)
		}
		return
	}
	bl, err := fsbinlog.NewFsBinlog(nil, binlog.Options{
		PrefixPath: argv.binlogPrefix,
		Magic:      BinlogMagic,
	})
	if err != nil {
		panic(err)
	}
	db, err := sqlitev2.OpenEngine(sqlitev2.Options{
		Path:   argv.dbPath,
		Scheme: schemeKV,
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()
	ln, err := rpc.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", argv.rpcPort), false)
	if err != nil {
		panic(err)
	}
	h := rpc_handler{
		readyCh: db.ReadyCh(),
		engine:  db,
	}
	eh := tlkv_engine.Handler{
		Get:    h.get,
		Inc:    h.incr,
		Put:    h.put,
		Backup: h.backup,
		Check:  h.Check,
	}
	rpcServer := rpc.NewServer(rpc.ServerWithHandler(eh.Handle))
	go func() {
		err := db.Run(bl, apply)
		if err != nil {
			panic(err)
		}
	}()
	err = db.WaitReady()
	if err != nil {
		panic(err)
	}
	err = rpcServer.Serve(ln)
	if err != nil {
		panic(err)
	}
}
