package serv

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/astaxie/beego/httplib"
	_ "github.com/eventials/go-tus"
	"github.com/sjqzhang/goutil"
	log "github.com/sjqzhang/seelog"
	"github.com/sjqzhang/tusd"
	"github.com/sjqzhang/tusd/filestore"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/thinxz-yuan/go-fastdfs/serv/cont"
	"github.com/thinxz-yuan/go-fastdfs/serv/ent"
	"io"
	"io/ioutil"
	slog "log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
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
		//
		server.formatStatInfo()
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
			// 自动检测系统状态 (文件和结点状态)
			server.checkFileAndSendToPeer(server.util.GetToDay(), cont.CONST_Md5_ERROR_FILE_NAME, false)
			//fmt.Println("CheckFileAndSendToPeer")
			time.Sleep(time.Second * time.Duration(Config().RefreshInterval))
			//server.util.RemoveEmptyDir(STORE_DIR)
		}
	}()

	//
	go server.cleanAndBackUp()
	//
	go server.checkClusterStatus()
	//
	go server.loadQueueSendToPeer()
	//
	go server.consumerPostToPeer()
	//
	go server.consumerLog()
	//
	go server.consumerDownLoad()
	//
	go server.consumerUpload()
	//
	go server.removeDownloading()
	//
	if Config().EnableFsnotify {
		go server.watchFilesChange()
	}
	// go server.loadSearchDict()
	//
	if Config().EnableMigrate {
		go server.repairFileInfoFromFile()
	}
	//
	if Config().AutoRepair {
		go func() {
			for {
				time.Sleep(time.Minute * 3)
				server.autoRepair(false)
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

// 检测并格式化, stat.json 状态文件
func (server *Server) formatStatInfo() {
	var (
		data  []byte
		err   error
		count int64
		stat  map[string]interface{}
	)
	if server.util.FileExists(cont.CONST_STAT_FILE_NAME) {
		if data, err = server.util.ReadBinFile(cont.CONST_STAT_FILE_NAME); err != nil {
			log.Error(err)
		} else {
			if err = json.Unmarshal(data, &stat); err != nil {
				log.Error(err)
			} else {
				for k, v := range stat {
					switch v.(type) {
					case float64:
						vv := strings.Split(fmt.Sprintf("%f", v), ".")[0]
						if count, err = strconv.ParseInt(vv, 10, 64); err != nil {
							log.Error(err)
						} else {
							server.statMap.Put(k, count)
						}
					default:
						server.statMap.Put(k, v)
					}
				}
			}
		}
	} else {
		server.RepairStatByDate(server.util.GetToDay())
	}
}

// 初始化 Tus
func (server *Server) initTus() {
	var (
		err     error
		fileLog *os.File
		bigDir  string
	)
	BIG_DIR := cont.STORE_DIR + "/_big/" + Config().PeerId
	os.MkdirAll(BIG_DIR, 0775)
	os.MkdirAll(cont.LOG_DIR, 0775)
	store := filestore.FileStore{
		Path: BIG_DIR,
	}
	if fileLog, err = os.OpenFile(cont.LOG_DIR+"/tusd.log", os.O_CREATE|os.O_RDWR, 0666); err != nil {
		log.Error(err)
		panic("initTus")
	}
	go func() {
		for {
			if fi, err := fileLog.Stat(); err != nil {
				log.Error(err)
			} else {
				if fi.Size() > 1024*1024*500 {
					//500M
					server.util.CopyFile(cont.LOG_DIR+"/tusd.log", cont.LOG_DIR+"/tusd.log.2")
					fileLog.Seek(0, 0)
					fileLog.Truncate(0)
					fileLog.Seek(0, 2)
				}
			}
			time.Sleep(time.Second * 30)
		}
	}()
	l := slog.New(fileLog, "[tusd] ", slog.LstdFlags)
	bigDir = cont.CONST_BIG_UPLOAD_PATH_SUFFIX
	if Config().SupportGroupManage {
		bigDir = fmt.Sprintf("/%s%s", Config().Group, cont.CONST_BIG_UPLOAD_PATH_SUFFIX)
	}
	composer := tusd.NewStoreComposer()
	// support raw tus upload and download
	store.GetReaderExt = func(id string) (io.Reader, error) {
		var (
			offset int64
			err    error
			length int
			buffer []byte
			fi     *ent.FileInfo
			fn     string
		)
		if fi, err = server.GetFileInfoFromLevelDB(id); err != nil {
			log.Error(err)
			return nil, err
		} else {
			if Config().AuthUrl != "" {
				fileResult := server.util.JsonEncodePretty(server.BuildFileResult(fi, nil))
				bufferReader := bytes.NewBuffer([]byte(fileResult))
				return bufferReader, nil
			}
			fn = fi.Name
			if fi.ReName != "" {
				fn = fi.ReName
			}
			fp := cont.DOCKER_DIR + fi.Path + "/" + fn
			if server.util.FileExists(fp) {
				log.Info(fmt.Sprintf("download:%s", fp))
				return os.Open(fp)
			}
			ps := strings.Split(fp, ",")
			if len(ps) > 2 && server.util.FileExists(ps[0]) {
				if length, err = strconv.Atoi(ps[2]); err != nil {
					return nil, err
				}
				if offset, err = strconv.ParseInt(ps[1], 10, 64); err != nil {
					return nil, err
				}
				if buffer, err = server.util.ReadFileByOffSet(ps[0], offset, length); err != nil {
					return nil, err
				}
				if buffer[0] == '1' {
					bufferReader := bytes.NewBuffer(buffer[1:])
					return bufferReader, nil
				} else {
					msg := "data no sync"
					log.Error(msg)
					return nil, errors.New(msg)
				}
			}
			return nil, errors.New(fmt.Sprintf("%s not found", fp))
		}
	}
	store.UseIn(composer)
	SetupPreHooks := func(composer *tusd.StoreComposer) {
		composer.UseCore(hookDataStore{
			DataStore: composer.Core,
		})
	}
	SetupPreHooks(composer)
	handler, err := tusd.NewHandler(tusd.Config{
		Logger:                  l,
		BasePath:                bigDir,
		StoreComposer:           composer,
		NotifyCompleteUploads:   true,
		RespectForwardedHeaders: true,
	})
	notify := func(handler *tusd.Handler) {
		for {
			select {
			case info := <-handler.CompleteUploads:
				log.Info("CompleteUploads", info)
				name := ""
				pathCustom := ""
				scene := Config().DefaultScene
				if v, ok := info.MetaData["filename"]; ok {
					name = v
				}
				if v, ok := info.MetaData["scene"]; ok {
					scene = v
				}
				if v, ok := info.MetaData["path"]; ok {
					pathCustom = v
				}
				var err error
				md5sum := ""
				oldFullPath := BIG_DIR + "/" + info.ID + ".bin"
				infoFullPath := BIG_DIR + "/" + info.ID + ".info"
				if md5sum, err = server.util.GetFileSumByName(oldFullPath, Config().FileSumArithmetic); err != nil {
					log.Error(err)
					continue
				}
				ext := path.Ext(name)
				filename := md5sum + ext
				if name != "" {
					filename = name
				}
				if Config().RenameFile {
					filename = md5sum + ext
				}
				timeStamp := time.Now().Unix()
				fpath := time.Now().Format("/20060102/15/04/")
				if pathCustom != "" {
					fpath = "/" + strings.Replace(pathCustom, ".", "", -1) + "/"
				}
				newFullPath := cont.STORE_DIR + "/" + scene + fpath + Config().PeerId + "/" + filename
				if pathCustom != "" {
					newFullPath = cont.STORE_DIR + "/" + scene + fpath + filename
				}
				if fi, err := server.GetFileInfoFromLevelDB(md5sum); err != nil {
					log.Error(err)
				} else {
					tpath := server.GetFilePathByInfo(fi, true)
					if fi.Md5 != "" && server.util.FileExists(tpath) {
						if _, err := server.SaveFileInfoToLevelDB(info.ID, fi, server.ldb); err != nil {
							log.Error(err)
						}
						log.Info(fmt.Sprintf("file is found md5:%s", fi.Md5))
						log.Info("remove file:", oldFullPath)
						log.Info("remove file:", infoFullPath)
						os.Remove(oldFullPath)
						os.Remove(infoFullPath)
						continue
					}
				}
				fpath = cont.STORE_DIR_NAME + "/" + Config().DefaultScene + fpath + Config().PeerId
				os.MkdirAll(cont.DOCKER_DIR+fpath, 0775)
				fileInfo := &ent.FileInfo{
					Name:      name,
					Path:      fpath,
					ReName:    filename,
					Size:      info.Size,
					TimeStamp: timeStamp,
					Md5:       md5sum,
					Peers:     []string{server.host},
					OffSet:    -1,
				}
				if err = os.Rename(oldFullPath, newFullPath); err != nil {
					log.Error(err)
					continue
				}
				log.Info(fileInfo)
				os.Remove(infoFullPath)
				if _, err = server.SaveFileInfoToLevelDB(info.ID, fileInfo, server.ldb); err != nil {
					//assosiate file id
					log.Error(err)
				}
				server.SaveFileMd5Log(fileInfo, cont.CONST_FILE_Md5_FILE_NAME)
				go server.postFileToPeer(fileInfo)
				callBack := func(info tusd.FileInfo, fileInfo *ent.FileInfo) {
					if callback_url, ok := info.MetaData["callback_url"]; ok {
						req := httplib.Post(callback_url)
						req.SetTimeout(time.Second*10, time.Second*10)
						req.Param("info", server.util.JsonEncodePretty(fileInfo))
						req.Param("id", info.ID)
						if _, err := req.String(); err != nil {
							log.Error(err)
						}
					}
				}
				go callBack(info, fileInfo)
			}
		}
	}
	go notify(handler)
	if err != nil {
		log.Error(err)
	}
	http.Handle(bigDir, http.StripPrefix(bigDir, handler))
}
