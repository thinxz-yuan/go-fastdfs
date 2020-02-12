package serv

import (
	"flag"
	"fmt"
	_ "github.com/eventials/go-tus"
	log "github.com/sjqzhang/seelog"
	"github.com/thinxz-yuan/go-fastdfs/common"
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

	// 全局服务对象
	global *Server
)

var (
	VERSION          string
	BUILD_TIME       string
	GO_VERSION       string
	GIT_VERSION      string
	CONST_QUEUE_SIZE = 10000
)

var (
	FOLDERS    = []string{DATA_DIR, STORE_DIR, CONF_DIR, STATIC_DIR}
	DATA_DIR   = cont.DATA_DIR_NAME
	STORE_DIR  = cont.STORE_DIR_NAME
	CONF_DIR   = cont.CONF_DIR_NAME
	STATIC_DIR = cont.STATIC_DIR_NAME
)

var (
	DOCKER_DIR                  = ""
	LOG_DIR                     = cont.LOG_DIR_NAME
	LARGE_DIR_NAME              = "haystack"
	LARGE_DIR                   = STORE_DIR + "/haystack"
	CONST_LEVELDB_FILE_NAME     = DATA_DIR + "/fileserver.db"
	CONST_LOG_LEVELDB_FILE_NAME = DATA_DIR + "/log.db"
	CONST_STAT_FILE_NAME        = DATA_DIR + "/stat.json"
	CONST_CONF_FILE_NAME        = CONF_DIR + "/cfg.json"
	CONST_SEARCH_FILE_NAME      = DATA_DIR + "/search.txt"
	CONST_UPLOAD_COUNTER_KEY    = "__CONST_UPLOAD_COUNTER_KEY__"
	LogConfigStr                = `
<seelog type="asynctimer" asyncinterval="1000" minlevel="trace" maxlevel="error">  
	<outputs formatid="common">  
		<buffered formatid="common" size="1048576" flushperiod="1000">  
			<rollingfile type="size" filename="{DOCKER_DIR}log/fileserver.log" maxsize="104857600" maxrolls="10"/>  
		</buffered>
	</outputs>  	  
	 <formats>
		 <format id="common" format="%Date %Time [%LEV] [%File:%Line] [%Func] %Msg%n" />  
	 </formats>  
</seelog>
`
	LogAccessConfigStr = `
<seelog type="asynctimer" asyncinterval="1000" minlevel="trace" maxlevel="error">  
	<outputs formatid="common">  
		<buffered formatid="common" size="1048576" flushperiod="1000">  
			<rollingfile type="size" filename="{DOCKER_DIR}log/access.log" maxsize="104857600" maxrolls="10"/>  
		</buffered>
	</outputs>  	  
	 <formats>
		 <format id="common" format="%Date %Time [%LEV] [%File:%Line] [%Func] %Msg%n" />  
	 </formats>  
</seelog>
`
)

func init() {
	flag.Parse()
	if *v {
		fmt.Printf("%s\n%s\n%s\n%s\n", VERSION, BUILD_TIME, GO_VERSION, GIT_VERSION)
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
	DOCKER_DIR = os.Getenv("GO_FASTDFS_DIR")
	if DOCKER_DIR != "" {
		if !strings.HasSuffix(DOCKER_DIR, "/") {
			DOCKER_DIR = DOCKER_DIR + "/"
		}
	}

	//
	STORE_DIR = DOCKER_DIR + cont.STORE_DIR_NAME
	CONF_DIR = DOCKER_DIR + cont.CONF_DIR_NAME
	DATA_DIR = DOCKER_DIR + cont.DATA_DIR_NAME
	LOG_DIR = DOCKER_DIR + cont.LOG_DIR_NAME
	STATIC_DIR = DOCKER_DIR + cont.STATIC_DIR_NAME
	LARGE_DIR_NAME = "haystack"
	LARGE_DIR = STORE_DIR + "/haystack"
	CONST_LEVELDB_FILE_NAME = DATA_DIR + "/fileserver.db"
	CONST_LOG_LEVELDB_FILE_NAME = DATA_DIR + "/log.db"
	CONST_STAT_FILE_NAME = DATA_DIR + "/stat.json"
	CONST_CONF_FILE_NAME = CONF_DIR + "/cfg.json"
	CONST_SEARCH_FILE_NAME = DATA_DIR + "/search.txt"
	FOLDERS = []string{DATA_DIR, STORE_DIR, CONF_DIR, STATIC_DIR}
	LogAccessConfigStr = strings.Replace(LogAccessConfigStr, "{DOCKER_DIR}", DOCKER_DIR, -1)
	LogConfigStr = strings.Replace(LogConfigStr, "{DOCKER_DIR}", DOCKER_DIR, -1)
	//
	for _, folder := range FOLDERS {
		os.MkdirAll(folder, 0775)
	}

	// 初始化时创建全局服务对象
	global, _ = NewServer()

	//
	peerId := fmt.Sprintf("%d", common.Util.RandInt(0, 9))
	if !common.Util.FileExists(CONST_CONF_FILE_NAME) {
		var ip string
		if ip = os.Getenv("GO_FASTDFS_IP"); ip == "" {
			ip = common.Util.GetPulicIP()
		}
		peer := "http://" + ip + ":8080"
		cfg := fmt.Sprintf(cont.CfgJson, peerId, peer, peer)
		common.Util.WriteFile(CONST_CONF_FILE_NAME, cfg)
	}

	//
	if logger, err := log.LoggerFromConfigAsBytes([]byte(LogConfigStr)); err != nil {
		panic(err)
	} else {
		log.ReplaceLogger(logger)
	}
	if _logger, err := log.LoggerFromConfigAsBytes([]byte(LogAccessConfigStr)); err == nil {
		logger = _logger
		log.Info("succes init log access")
	} else {
		log.Error(err.Error())
	}

	//
	common.ParseConfig(CONST_CONF_FILE_NAME)
	if common.Config().QueueSize == 0 {
		common.Config().QueueSize = CONST_QUEUE_SIZE
	}
	if common.Config().PeerId == "" {
		common.Config().PeerId = peerId
	}
	if common.Config().SupportGroupManage {
		staticHandler = http.StripPrefix("/"+common.Config().Group+"/", http.FileServer(http.Dir(STORE_DIR)))
	} else {
		staticHandler = http.StripPrefix("/", http.FileServer(http.Dir(STORE_DIR)))
	}

	// 相关参数配置初始化
	global.initComponent(false)
}
