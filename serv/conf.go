package serv

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"unsafe"

	_ "github.com/eventials/go-tus"
	log "github.com/sjqzhang/seelog"
	"github.com/thinxz-yuan/go-fastdfs/cont"
	"github.com/thinxz-yuan/go-fastdfs/ent"
)

var logacc log.LoggerInterface

var v = flag.Bool("v", false, "display version")

var ptr unsafe.Pointer

func init() {
	flag.Parse()
	if *v {
		fmt.Printf("%s\n%s\n%s\n%s\n", cont.VERSION, cont.BUILD_TIME, cont.GO_VERSION, cont.GIT_VERSION)
		os.Exit(0)
	}
	appDir, e1 := filepath.Abs(filepath.Dir(os.Args[0]))
	curDir, e2 := filepath.Abs(".")
	if e1 == nil && e2 == nil && appDir != curDir {
		msg := fmt.Sprintf("please change directory to '%s' start fileserver\n", appDir)
		msg = msg + fmt.Sprintf("请切换到 '%s' 目录启动 fileserver ", appDir)
		log.Warn(msg)
		fmt.Println(msg)
		os.Exit(1)
	}
	cont.DOCKER_DIR = os.Getenv("GO_FASTDFS_DIR")
	if cont.DOCKER_DIR != "" {
		if !strings.HasSuffix(cont.DOCKER_DIR, "/") {
			cont.DOCKER_DIR = cont.DOCKER_DIR + "/"
		}
	}
	cont.STORE_DIR = cont.DOCKER_DIR + cont.STORE_DIR_NAME
	cont.CONF_DIR = cont.DOCKER_DIR + cont.CONF_DIR_NAME
	cont.DATA_DIR = cont.DOCKER_DIR + cont.DATA_DIR_NAME
	cont.LOG_DIR = cont.DOCKER_DIR + cont.LOG_DIR_NAME
	cont.STATIC_DIR = cont.DOCKER_DIR + cont.STATIC_DIR_NAME
	cont.LARGE_DIR_NAME = "haystack"
	cont.LARGE_DIR = cont.STORE_DIR + "/haystack"
	cont.CONST_LEVELDB_FILE_NAME = cont.DATA_DIR + "/fileserver.db"
	cont.CONST_LOG_LEVELDB_FILE_NAME = cont.DATA_DIR + "/log.db"
	cont.CONST_STAT_FILE_NAME = cont.DATA_DIR + "/stat.json"
	cont.CONST_CONF_FILE_NAME = cont.CONF_DIR + "/cfg.json"
	cont.CONST_SEARCH_FILE_NAME = cont.DATA_DIR + "/search.txt"
	cont.FOLDERS = []string{cont.DATA_DIR, cont.STORE_DIR, cont.CONF_DIR, cont.STATIC_DIR}
	cont.LogAccessConfigStr = strings.Replace(cont.LogAccessConfigStr, "{DOCKER_DIR}", cont.DOCKER_DIR, -1)
	cont.LogConfigStr = strings.Replace(cont.LogConfigStr, "{DOCKER_DIR}", cont.DOCKER_DIR, -1)
	for _, folder := range cont.FOLDERS {
		os.MkdirAll(folder, 0775)
	}
	server = NewServer()

	peerId := fmt.Sprintf("%d", server.util.RandInt(0, 9))
	if !server.util.FileExists(cont.CONST_CONF_FILE_NAME) {
		var ip string
		if ip = os.Getenv("GO_FASTDFS_IP"); ip == "" {
			ip = server.util.GetPulicIP()
		}
		peer := "http://" + ip + ":8080"
		cfg := fmt.Sprintf(cont.CfgJson, peerId, peer, peer)
		server.util.WriteFile(cont.CONST_CONF_FILE_NAME, cfg)
	}
	if logger, err := log.LoggerFromConfigAsBytes([]byte(cont.LogConfigStr)); err != nil {
		panic(err)
	} else {
		log.ReplaceLogger(logger)
	}
	if _logacc, err := log.LoggerFromConfigAsBytes([]byte(cont.LogAccessConfigStr)); err == nil {
		logacc = _logacc
		log.Info("succes init log access")
	} else {
		log.Error(err.Error())
	}
	ParseConfig(cont.CONST_CONF_FILE_NAME)
	if Config().QueueSize == 0 {
		Config().QueueSize = cont.CONST_QUEUE_SIZE
	}
	if Config().PeerId == "" {
		Config().PeerId = peerId
	}
	if Config().SupportGroupManage {
		staticHandler = http.StripPrefix("/"+Config().Group+"/", http.FileServer(http.Dir(cont.STORE_DIR)))
	} else {
		staticHandler = http.StripPrefix("/", http.FileServer(http.Dir(cont.STORE_DIR)))
	}
	server.initComponent(false)
}

func Config() *ent.GloablConfig {
	return (*ent.GloablConfig)(atomic.LoadPointer(&ptr))
}

func ParseConfig(filePath string) {
	var (
		data []byte
	)
	if filePath == "" {
		data = []byte(strings.TrimSpace(cont.CfgJson))
	} else {
		file, err := os.Open(filePath)
		if err != nil {
			panic(fmt.Sprintln("open file path:", filePath, "error:", err))
		}
		defer file.Close()
		cont.FileName = filePath
		data, err = ioutil.ReadAll(file)
		if err != nil {
			panic(fmt.Sprintln("file path:", filePath, " read all error:", err))
		}
	}
	var c ent.GloablConfig
	if err := json.Unmarshal(data, &c); err != nil {
		panic(fmt.Sprintln("file path:", filePath, "json unmarshal error:", err))
	}
	log.Info(c)
	atomic.StorePointer(&ptr, unsafe.Pointer(&c))
	log.Info("config parse success")
}
