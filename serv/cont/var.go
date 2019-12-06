package cont

var FOLDERS = []string{DATA_DIR, STORE_DIR, CONF_DIR, STATIC_DIR}

var CONST_QUEUE_SIZE = 10000

var (
	VERSION     string
	BUILD_TIME  string
	GO_VERSION  string
	GIT_VERSION string
)

var (
	FileName                    string
	DOCKER_DIR                  = ""
	STORE_DIR                   = STORE_DIR_NAME
	CONF_DIR                    = CONF_DIR_NAME
	LOG_DIR                     = LOG_DIR_NAME
	DATA_DIR                    = DATA_DIR_NAME
	STATIC_DIR                  = STATIC_DIR_NAME
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
