package serv

import (
	"flag"
	"fmt"
	_ "github.com/eventials/go-tus"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sjqzhang/seelog"
	"github.com/thinxz-yuan/go-fastdfs/serv/cont"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
)

var (
	// 日志
	logger log.LoggerInterface
	//
	v = flag.Bool("v", false, "display version")
	//
	staticHandler http.Handler
	//
	json = jsoniter.ConfigCompatibleWithStandardLibrary

	// 全局服务对象
	global *Server
)

func init() {
	flag.Parse()
	if *v {
		fmt.Printf("%s\n%s\n%s\n%s\n", cont.VERSION, cont.BUILD_TIME, cont.GO_VERSION, cont.GIT_VERSION)
		os.Exit(0)
	}

	//
	appDir, e1 := filepath.Abs(filepath.Dir(os.Args[0]))
	curDir, e2 := filepath.Abs(".")
	if e1 == nil && e2 == nil && appDir != curDir {
		msg := fmt.Sprintf("please change directory to '%s' start fileserver\n", appDir)
		msg = msg + fmt.Sprintf("请切换到 '%s' 目录启动 fileserver ", appDir)
		log.Warn(msg)
		fmt.Println(msg)
		os.Exit(1)
	}

	//
	cont.DOCKER_DIR = os.Getenv("GO_FASTDFS_DIR")
	if cont.DOCKER_DIR != "" {
		if !strings.HasSuffix(cont.DOCKER_DIR, "/") {
			cont.DOCKER_DIR = cont.DOCKER_DIR + "/"
		}
	}

	//
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
	//
	for _, folder := range cont.FOLDERS {
		os.MkdirAll(folder, 0775)
	}

	// 初始化时创建全局服务对象
	global, _ = NewServer()

	//
	peerId := fmt.Sprintf("%d", global.util.RandInt(0, 9))
	if !global.util.FileExists(cont.CONST_CONF_FILE_NAME) {
		var ip string
		if ip = os.Getenv("GO_FASTDFS_IP"); ip == "" {
			ip = global.util.GetPulicIP()
		}
		peer := "http://" + ip + ":8080"
		cfg := fmt.Sprintf(cont.CfgJson, peerId, peer, peer)
		global.util.WriteFile(cont.CONST_CONF_FILE_NAME, cfg)
	}

	//
	if logger, err := log.LoggerFromConfigAsBytes([]byte(cont.LogConfigStr)); err != nil {
		panic(err)
	} else {
		log.ReplaceLogger(logger)
	}
	if _logger, err := log.LoggerFromConfigAsBytes([]byte(cont.LogAccessConfigStr)); err == nil {
		logger = _logger
		log.Info("succes init log access")
	} else {
		log.Error(err.Error())
	}

	//
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

	//
	global.initComponent(false)
}
