package serv

import (
	"fmt"
	"github.com/astaxie/beego/httplib"
	_ "github.com/eventials/go-tus"
	"github.com/sjqzhang/goutil"
	log "github.com/sjqzhang/seelog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/thinxz-yuan/go-fastdfs/serv/cont"
	"github.com/thinxz-yuan/go-fastdfs/serv/ent"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

type Server struct {
	ldb            *leveldb.DB
	logDB          *leveldb.DB
	util           *goutil.Common
	statMap        *goutil.CommonMap
	sumMap         *goutil.CommonMap
	rtMap          *goutil.CommonMap
	queueToPeers   chan ent.FileInfo
	queueFromPeers chan ent.FileInfo
	queueFileLog   chan *ent.FileLog
	queueUpload    chan ent.WrapReqResp
	lockMap        *goutil.CommonMap
	sceneMap       *goutil.CommonMap
	searchMap      *goutil.CommonMap
	curDate        string
	host           string
}

func NewServer() (server *Server, err error) {
	server = &Server{
		util:           &goutil.Common{},
		statMap:        goutil.NewCommonMap(0),
		lockMap:        goutil.NewCommonMap(0),
		rtMap:          goutil.NewCommonMap(0),
		sceneMap:       goutil.NewCommonMap(0),
		searchMap:      goutil.NewCommonMap(0),
		queueToPeers:   make(chan ent.FileInfo, cont.CONST_QUEUE_SIZE),
		queueFromPeers: make(chan ent.FileInfo, cont.CONST_QUEUE_SIZE),
		queueFileLog:   make(chan *ent.FileLog, cont.CONST_QUEUE_SIZE),
		queueUpload:    make(chan ent.WrapReqResp, 100),
		sumMap:         goutil.NewCommonMap(365 * 3),
	}

	defaultTransport := &http.Transport{
		DisableKeepAlives:   true,
		Dial:                httplib.TimeoutDialer(time.Second*15, time.Second*300),
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
	}
	settings := httplib.BeegoHTTPSettings{
		UserAgent:        "Go-FastDFS",
		ConnectTimeout:   15 * time.Second,
		ReadWriteTimeout: 15 * time.Second,
		Gzip:             true,
		DumpBody:         true,
		Transport:        defaultTransport,
	}

	httplib.SetDefaultSetting(settings)
	server.statMap.Put(cont.CONST_STAT_FILE_COUNT_KEY, int64(0))
	server.statMap.Put(cont.CONST_STAT_FILE_TOTAL_SIZE_KEY, int64(0))
	server.statMap.Put(server.util.GetToDay()+"_"+cont.CONST_STAT_FILE_COUNT_KEY, int64(0))
	server.statMap.Put(server.util.GetToDay()+"_"+cont.CONST_STAT_FILE_TOTAL_SIZE_KEY, int64(0))
	server.curDate = server.util.GetToDay()
	opts := &opt.Options{
		CompactionTableSize: 1024 * 1024 * 20,
		WriteBuffer:         1024 * 1024 * 20,
	}

	//
	if server.ldb, err = leveldb.OpenFile(cont.CONST_LEVELDB_FILE_NAME, opts); err != nil {
		fmt.Println(fmt.Sprintf("open db file %s fail,maybe has opening", cont.CONST_LEVELDB_FILE_NAME))
		log.Error(err)
		panic(err)
	}

	//
	server.logDB, err = leveldb.OpenFile(cont.CONST_LOG_LEVELDB_FILE_NAME, opts)
	if err != nil {
		fmt.Println(fmt.Sprintf("open db file %s fail,maybe has opening", cont.CONST_LOG_LEVELDB_FILE_NAME))
		log.Error(err)
		panic(err)

	}

	return server, nil
}

func (server *Server) CheckFileExistByInfo(md5s string, fileInfo *ent.FileInfo) bool {
	var (
		err      error
		fullpath string
		fi       os.FileInfo
		info     *ent.FileInfo
	)
	if fileInfo == nil {
		return false
	}
	if fileInfo.OffSet >= 0 {
		//small file
		if info, err = server.GetFileInfoFromLevelDB(fileInfo.Md5); err == nil && info.Md5 == fileInfo.Md5 {
			return true
		} else {
			return false
		}
	}
	fullpath = server.GetFilePathByInfo(fileInfo, true)
	if fi, err = os.Stat(fullpath); err != nil {
		return false
	}
	if fi.Size() == fileInfo.Size {
		return true
	} else {
		return false
	}
}
func (server *Server) CrossOrigin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, X-File-Type, Cache-Control, Origin")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Expose-Headers", "Authorization")
	//https://blog.csdn.net/yanzisu_congcong/article/details/80552155
}
func (server *Server) IsExistFromLevelDB(key string, db *leveldb.DB) (bool, error) {
	return db.Has([]byte(key), nil)
}
func (server *Server) GetFileInfoFromLevelDB(key string) (*ent.FileInfo, error) {
	var (
		err      error
		data     []byte
		fileInfo ent.FileInfo
	)
	if data, err = server.ldb.Get([]byte(key), nil); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(data, &fileInfo); err != nil {
		return nil, err
	}
	return &fileInfo, nil
}
func (server *Server) SaveStat() {
	SaveStatFunc := func() {
		defer func() {
			if re := recover(); re != nil {
				buffer := debug.Stack()
				log.Error("SaveStatFunc")
				log.Error(re)
				log.Error(string(buffer))
			}
		}()
		stat := server.statMap.Get()
		if v, ok := stat[cont.CONST_STAT_FILE_COUNT_KEY]; ok {
			switch v.(type) {
			case int64, int32, int, float64, float32:
				if v.(int64) >= 0 {
					if data, err := json.Marshal(stat); err != nil {
						log.Error(err)
					} else {
						server.util.WriteBinFile(cont.CONST_STAT_FILE_NAME, data)
					}
				}
			}
		}
	}
	SaveStatFunc()
}
func (server *Server) IsPeer(r *http.Request) bool {
	var (
		ip    string
		peer  string
		bflag bool
	)
	//return true
	ip = server.util.GetClientIp(r)
	realIp := os.Getenv("GO_FASTDFS_IP")
	if realIp == "" {
		realIp = server.util.GetPulicIP()
	}
	if ip == "127.0.0.1" || ip == realIp {
		return true
	}
	if server.util.Contains(ip, Config().AdminIps) {
		return true
	}
	ip = "http://" + ip
	bflag = false
	for _, peer = range Config().Peers {
		if strings.HasPrefix(peer, ip) {
			bflag = true
			break
		}
	}
	return bflag
}
func (server *Server) GetClusterNotPermitMessage(r *http.Request) string {
	var (
		message string
	)
	message = fmt.Sprintf(cont.CONST_MESSAGE_CLUSTER_IP, server.util.GetClientIp(r))
	return message
}

func Start() {
	//
	global.startComponent()
}

// 相关参数配置初始化
// ---------- ---------- ----------
// isReload 是否为重新加载
// ---------- ---------- ----------
func (server *Server) initComponent(isReload bool) {
	//
	var ip string
	if ip := os.Getenv("GO_FASTDFS_IP"); ip == "" {
		ip = server.util.GetPulicIP()
	}
	if Config().Host == "" {
		if len(strings.Split(Config().Addr, ":")) == 2 {
			server.host = fmt.Sprintf("http://%s:%s", ip, strings.Split(Config().Addr, ":")[1])
			Config().Host = server.host
		}
	} else {
		if strings.HasPrefix(Config().Host, "http") {
			server.host = Config().Host
		} else {
			server.host = "http://" + Config().Host
		}
	}

	//
	ex, _ := regexp.Compile("\\d+\\.\\d+\\.\\d+\\.\\d+")
	var peers []string
	for _, peer := range Config().Peers {
		if server.util.Contains(ip, ex.FindAllString(peer, -1)) ||
			server.util.Contains("127.0.0.1", ex.FindAllString(peer, -1)) {
			continue
		}
		if strings.HasPrefix(peer, "http") {
			peers = append(peers, peer)
		} else {
			peers = append(peers, "http://"+peer)
		}
	}
	Config().Peers = peers

	//
	if !isReload {
		server.FormatStatInfo()
		if Config().EnableTus {
			server.initTus()
		}
	}

	//
	for _, s := range Config().Scenes {
		kv := strings.Split(s, ":")
		if len(kv) == 2 {
			server.sceneMap.Put(kv[0], kv[1])
		}
	}
	if Config().ReadTimeout == 0 {
		Config().ReadTimeout = 60 * 10
	}
	if Config().WriteTimeout == 0 {
		Config().WriteTimeout = 60 * 10
	}
	if Config().SyncWorker == 0 {
		Config().SyncWorker = 200
	}
	if Config().UploadWorker == 0 {
		Config().UploadWorker = runtime.NumCPU() + 4
		if runtime.NumCPU() < 4 {
			Config().UploadWorker = 8
		}
	}
	if Config().UploadQueueSize == 0 {
		Config().UploadQueueSize = 200
	}
	if Config().RetryCount == 0 {
		Config().RetryCount = 3
	}
}

// 启动服务组件
// ---------- ---------- ----------
func (server *Server) startComponent() {
	go func() {
		for {
			//
			server.CheckFileAndSendToPeer(server.util.GetToDay(), cont.CONST_Md5_ERROR_FILE_NAME, false)
			//fmt.Println("CheckFileAndSendToPeer")
			time.Sleep(time.Second * time.Duration(Config().RefreshInterval))
			//server.util.RemoveEmptyDir(STORE_DIR)
		}
	}()

	//
	go server.CleanAndBackUp()
	go server.CheckClusterStatus()
	go server.LoadQueueSendToPeer()
	go server.ConsumerPostToPeer()
	go server.ConsumerLog()
	go server.ConsumerDownLoad()
	go server.ConsumerUpload()
	go server.RemoveDownloading()
	//
	if Config().EnableFsnotify {
		go server.WatchFilesChange()
	}
	// go server.LoadSearchDict()
	//
	if Config().EnableMigrate {
		go server.RepairFileInfoFromFile()
	}
	//
	if Config().AutoRepair {
		go func() {
			for {
				time.Sleep(time.Minute * 3)
				server.AutoRepair(false)
				time.Sleep(time.Minute * 60)
			}
		}()
	}

	//
	groupRoute := ""
	if Config().SupportGroupManage {
		groupRoute = "/" + Config().Group
	}
	//
	go func() { // force free memory
		for {
			time.Sleep(time.Minute * 1)
			debug.FreeOSMemory()
		}
	}()

	//
	server.startHttpServe(groupRoute)
}

func (server *Server) startHttpServe(groupRoute string) {
	//
	uploadPage := "upload.html"
	if groupRoute == "" {
		http.HandleFunc(fmt.Sprintf("%s", "/"), server.Download)
		http.HandleFunc(fmt.Sprintf("/%s", uploadPage), server.Index)
	} else {
		http.HandleFunc(fmt.Sprintf("%s", "/"), server.Download)
		http.HandleFunc(fmt.Sprintf("%s", groupRoute), server.Download)
		http.HandleFunc(fmt.Sprintf("%s/%s", groupRoute, uploadPage), server.Index)
	}

	http.HandleFunc(fmt.Sprintf("%s/check_files_exist", groupRoute), server.CheckFilesExist)
	http.HandleFunc(fmt.Sprintf("%s/check_file_exist", groupRoute), server.CheckFileExist)
	http.HandleFunc(fmt.Sprintf("%s/upload", groupRoute), server.Upload)
	http.HandleFunc(fmt.Sprintf("%s/delete", groupRoute), server.RemoveFile)
	http.HandleFunc(fmt.Sprintf("%s/get_file_info", groupRoute), server.GetFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/sync", groupRoute), server.Sync)
	http.HandleFunc(fmt.Sprintf("%s/stat", groupRoute), server.Stat)
	http.HandleFunc(fmt.Sprintf("%s/repair_stat", groupRoute), server.RepairStatWeb)
	http.HandleFunc(fmt.Sprintf("%s/status", groupRoute), server.Status)
	http.HandleFunc(fmt.Sprintf("%s/repair", groupRoute), server.Repair)
	http.HandleFunc(fmt.Sprintf("%s/report", groupRoute), server.Report)
	http.HandleFunc(fmt.Sprintf("%s/backup", groupRoute), server.BackUp)
	http.HandleFunc(fmt.Sprintf("%s/search", groupRoute), server.Search)
	http.HandleFunc(fmt.Sprintf("%s/list_dir", groupRoute), server.ListDir)
	http.HandleFunc(fmt.Sprintf("%s/remove_empty_dir", groupRoute), server.RemoveEmptyDir)
	http.HandleFunc(fmt.Sprintf("%s/repair_fileinfo", groupRoute), server.RepairFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/reload", groupRoute), server.reload)
	http.HandleFunc(fmt.Sprintf("%s/syncfile_info", groupRoute), server.SyncFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/get_md5s_by_date", groupRoute), server.GetMd5sForWeb)
	http.HandleFunc(fmt.Sprintf("%s/receive_md5s", groupRoute), server.ReceiveMd5s)
	http.HandleFunc(fmt.Sprintf("%s/gen_google_secret", groupRoute), server.GenGoogleSecret)
	http.HandleFunc(fmt.Sprintf("%s/gen_google_code", groupRoute), server.GenGoogleCode)
	http.HandleFunc("/"+Config().Group+"/", server.Download)

	//
	fmt.Println("Listen on " + Config().Addr)
	srv := &http.Server{
		Addr:              Config().Addr,
		Handler:           new(HttpHandler),
		ReadTimeout:       time.Duration(Config().ReadTimeout) * time.Second,
		ReadHeaderTimeout: time.Duration(Config().ReadHeaderTimeout) * time.Second,
		WriteTimeout:      time.Duration(Config().WriteTimeout) * time.Second,
		IdleTimeout:       time.Duration(Config().IdleTimeout) * time.Second,
	}

	// 开启HTTP服务, (阻塞主线程)
	err := srv.ListenAndServe()

	//
	_ = log.Error(err)
	fmt.Println(err)
}

// 重启初始化加载
// ------------------------------------
func (server *Server) reload(w http.ResponseWriter, r *http.Request) {
	var (
		data   []byte
		cfg    GlobalConfig
		result ent.JsonResult
	)
	result.Status = "fail"
	err := r.ParseForm()
	if !server.IsPeer(r) {
		w.Write([]byte(server.GetClusterNotPermitMessage(r)))
		return
	}
	cfgJson := r.FormValue("cfg")
	action := r.FormValue("action")

	//
	if action == "get" {
		result.Data = Config()
		result.Status = "ok"
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	//
	if action == "set" {
		if cfgJson == "" {
			result.Message = "(error)parameter cfg(json) require"
			w.Write([]byte(server.util.JsonEncodePretty(result)))
			return
		}
		if err = json.Unmarshal([]byte(cfgJson), &cfg); err != nil {
			log.Error(err)
			result.Message = err.Error()
			w.Write([]byte(server.util.JsonEncodePretty(result)))
			return
		}
		result.Status = "ok"
		cfgJson = server.util.JsonEncodePretty(cfg)
		server.util.WriteFile(cont.CONST_CONF_FILE_NAME, cfgJson)
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	//
	if action == "reload" {
		if data, err = ioutil.ReadFile(cont.CONST_CONF_FILE_NAME); err != nil {
			result.Message = err.Error()
			w.Write([]byte(server.util.JsonEncodePretty(result)))
			return
		}
		if err = json.Unmarshal(data, &cfg); err != nil {
			result.Message = err.Error()
			_, err = w.Write([]byte(server.util.JsonEncodePretty(result)))
			return
		}
		ParseConfig(cont.CONST_CONF_FILE_NAME)
		server.initComponent(true)
		result.Status = "ok"
		_, err = w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	//
	if action == "" {
		_, err = w.Write([]byte("(error)action support set(json) get reload"))
	}
}
