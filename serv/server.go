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
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/thinxz-yuan/go-fastdfs/common"
	"github.com/thinxz-yuan/go-fastdfs/serv/cont"
	"github.com/thinxz-yuan/go-fastdfs/serv/web"
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
	statMap        *goutil.CommonMap
	sumMap         *goutil.CommonMap
	rtMap          *goutil.CommonMap
	queueToPeers   chan common.FileInfo
	queueFromPeers chan common.FileInfo
	queueFileLog   chan *common.FileLog
	queueUpload    chan common.WrapReqResp
	lockMap        *goutil.CommonMap
	sceneMap       *goutil.CommonMap
	searchMap      *goutil.CommonMap
	curDate        string
	host           string
	groupName      string
}

func NewServer() (server *Server, err error) {
	server = &Server{
		statMap:        goutil.NewCommonMap(0),
		lockMap:        goutil.NewCommonMap(0),
		rtMap:          goutil.NewCommonMap(0),
		sceneMap:       goutil.NewCommonMap(0),
		searchMap:      goutil.NewCommonMap(0),
		queueToPeers:   make(chan common.FileInfo, CONST_QUEUE_SIZE),
		queueFromPeers: make(chan common.FileInfo, CONST_QUEUE_SIZE),
		queueFileLog:   make(chan *common.FileLog, CONST_QUEUE_SIZE),
		queueUpload:    make(chan common.WrapReqResp, 100),
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
	server.statMap.Put(common.Util.GetToDay()+"_"+cont.CONST_STAT_FILE_COUNT_KEY, int64(0))
	server.statMap.Put(common.Util.GetToDay()+"_"+cont.CONST_STAT_FILE_TOTAL_SIZE_KEY, int64(0))
	server.curDate = common.Util.GetToDay()
	opts := &opt.Options{
		CompactionTableSize: 1024 * 1024 * 20,
		WriteBuffer:         1024 * 1024 * 20,
	}

	//
	if server.ldb, err = leveldb.OpenFile(CONST_LEVELDB_FILE_NAME, opts); err != nil {
		fmt.Println(fmt.Sprintf("open db file %s fail,maybe has opening", CONST_LEVELDB_FILE_NAME))
		log.Error(err)
		panic(err)
	}

	//
	server.logDB, err = leveldb.OpenFile(CONST_LOG_LEVELDB_FILE_NAME, opts)
	if err != nil {
		fmt.Println(fmt.Sprintf("open db file %s fail,maybe has opening", CONST_LOG_LEVELDB_FILE_NAME))
		log.Error(err)
		panic(err)

	}

	return server, nil
}

func (server *Server) CheckFileExistByInfo(md5s string, fileInfo *common.FileInfo) bool {
	var (
		err      error
		fullpath string
		fi       os.FileInfo
		info     *common.FileInfo
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
func (server *Server) IsExistFromLevelDB(key string, db *leveldb.DB) (bool, error) {
	return db.Has([]byte(key), nil)
}
func (server *Server) GetFileInfoFromLevelDB(key string) (*common.FileInfo, error) {
	var (
		err      error
		data     []byte
		fileInfo common.FileInfo
	)
	if data, err = server.ldb.Get([]byte(key), nil); err != nil {
		return nil, err
	}
	if err = common.JSON.Unmarshal(data, &fileInfo); err != nil {
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
					if data, err := common.JSON.Marshal(stat); err != nil {
						log.Error(err)
					} else {
						common.Util.WriteBinFile(CONST_STAT_FILE_NAME, data)
					}
				}
			}
		}
	}
	SaveStatFunc()
}

//
func (server *Server) GetLogger() log.LoggerInterface {
	return logger
}

func (server *Server) GroupName() string {
	return server.groupName
}

func Start() *Server {
	//
	global.startComponent()
	return global
}

// 相关参数配置初始化
// ---------- ---------- ----------
// isReload 是否为重新加载
// ---------- ---------- ----------
func (server *Server) initComponent(isReload bool) {
	//
	var ip string
	if ip := os.Getenv("GO_FASTDFS_IP"); ip == "" {
		ip = common.Util.GetPulicIP()
	}
	if common.Config().Host == "" {
		if len(strings.Split(common.Config().Addr, ":")) == 2 {
			server.host = fmt.Sprintf("http://%s:%s", ip, strings.Split(common.Config().Addr, ":")[1])
			common.Config().Host = server.host
		}
	} else {
		if strings.HasPrefix(common.Config().Host, "http") {
			server.host = common.Config().Host
		} else {
			server.host = "http://" + common.Config().Host
		}
	}

	//
	ex, _ := regexp.Compile("\\d+\\.\\d+\\.\\d+\\.\\d+")
	var peers []string
	for _, peer := range common.Config().Peers {
		if common.Util.Contains(ip, ex.FindAllString(peer, -1)) ||
			common.Util.Contains("127.0.0.1", ex.FindAllString(peer, -1)) {
			continue
		}
		if strings.HasPrefix(peer, "http") {
			peers = append(peers, peer)
		} else {
			peers = append(peers, "http://"+peer)
		}
	}
	common.Config().Peers = peers

	//
	if !isReload {
		//
		server.formatStatInfo()
		if common.Config().EnableTus {
			server.initTus()
		}
	}

	//
	for _, s := range common.Config().Scenes {
		kv := strings.Split(s, ":")
		if len(kv) == 2 {
			server.sceneMap.Put(kv[0], kv[1])
		}
	}
	if common.Config().ReadTimeout == 0 {
		common.Config().ReadTimeout = 60 * 10
	}
	if common.Config().WriteTimeout == 0 {
		common.Config().WriteTimeout = 60 * 10
	}
	if common.Config().SyncWorker == 0 {
		common.Config().SyncWorker = 200
	}
	if common.Config().UploadWorker == 0 {
		common.Config().UploadWorker = runtime.NumCPU() + 4
		if runtime.NumCPU() < 4 {
			common.Config().UploadWorker = 8
		}
	}
	if common.Config().UploadQueueSize == 0 {
		common.Config().UploadQueueSize = 200
	}
	if common.Config().RetryCount == 0 {
		common.Config().RetryCount = 3
	}
}

// 启动服务组件
// ---------- ---------- ----------
func (server *Server) startComponent() {
	go func() {
		for {
			// 自动检测系统状态 (文件和结点状态)
			server.checkFileAndSendToPeer(common.Util.GetToDay(), cont.CONST_Md5_ERROR_FILE_NAME, false)
			//fmt.Println("CheckFileAndSendToPeer")
			time.Sleep(time.Second * time.Duration(common.Config().RefreshInterval))
			//common.Util.RemoveEmptyDir(STORE_DIR)
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
	if common.Config().EnableFsnotify {
		go server.watchFilesChange()
	}
	// go server.loadSearchDict()
	//
	if common.Config().EnableMigrate {
		go server.repairFileInfoFromFile()
	}
	//
	if common.Config().AutoRepair {
		go func() {
			for {
				time.Sleep(time.Minute * 3)
				server.autoRepair(false)
				time.Sleep(time.Minute * 60)
			}
		}()
	}

	groupRoute := ""
	if common.Config().SupportGroupManage {
		groupRoute = "/" + common.Config().Group
	}
	server.groupName = groupRoute

	go func() { // force free memory
		for {
			time.Sleep(time.Minute * 1)
			debug.FreeOSMemory()
		}
	}()
}

// 重启初始化加载
// ------------------------------------
func (server *Server) Reload(w http.ResponseWriter, r *http.Request) {
	var (
		data   []byte
		cfg    common.GlobalConfig
		result common.JsonResult
	)
	result.Status = "fail"
	err := r.ParseForm()
	if !common.IsPeer(r) {
		w.Write([]byte(common.GetClusterNotPermitMessage(r)))
		return
	}
	cfgJson := r.FormValue("cfg")
	action := r.FormValue("action")

	//
	if action == "get" {
		result.Data = common.Config()
		result.Status = "ok"
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	//
	if action == "set" {
		if cfgJson == "" {
			result.Message = "(error)parameter cfg(json) require"
			w.Write([]byte(common.Util.JsonEncodePretty(result)))
			return
		}
		if err = common.JSON.Unmarshal([]byte(cfgJson), &cfg); err != nil {
			log.Error(err)
			result.Message = err.Error()
			w.Write([]byte(common.Util.JsonEncodePretty(result)))
			return
		}
		result.Status = "ok"
		cfgJson = common.Util.JsonEncodePretty(cfg)
		common.Util.WriteFile(CONST_CONF_FILE_NAME, cfgJson)
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	//
	if action == "reload" {
		if data, err = ioutil.ReadFile(CONST_CONF_FILE_NAME); err != nil {
			result.Message = err.Error()
			w.Write([]byte(common.Util.JsonEncodePretty(result)))
			return
		}
		if err = common.JSON.Unmarshal(data, &cfg); err != nil {
			result.Message = err.Error()
			_, err = w.Write([]byte(common.Util.JsonEncodePretty(result)))
			return
		}
		common.ParseConfig(CONST_CONF_FILE_NAME)
		server.initComponent(true)
		result.Status = "ok"
		_, err = w.Write([]byte(common.Util.JsonEncodePretty(result)))
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
	if common.Util.FileExists(CONST_STAT_FILE_NAME) {
		if data, err = common.Util.ReadBinFile(CONST_STAT_FILE_NAME); err != nil {
			log.Error(err)
		} else {
			if err = common.JSON.Unmarshal(data, &stat); err != nil {
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
		server.repairStatByDate(common.Util.GetToDay())
	}
}

func (server *Server) repairStatByDate(date string) common.StatDateFileInfo {
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("RepairStatByDate")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	var (
		err       error
		keyPrefix string
		fileInfo  common.FileInfo
		fileCount int64
		fileSize  int64
		stat      common.StatDateFileInfo
	)
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, cont.CONST_FILE_Md5_FILE_NAME)
	iter := server.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter.Release()
	for iter.Next() {
		if err = common.JSON.Unmarshal(iter.Value(), &fileInfo); err != nil {
			continue
		}
		fileCount = fileCount + 1
		fileSize = fileSize + fileInfo.Size
	}
	server.statMap.Put(date+"_"+cont.CONST_STAT_FILE_COUNT_KEY, fileCount)
	server.statMap.Put(date+"_"+cont.CONST_STAT_FILE_TOTAL_SIZE_KEY, fileSize)
	server.SaveStat()
	stat.Date = date
	stat.FileCount = fileCount
	stat.TotalSize = fileSize
	return stat
}

// 初始化 Tus
func (server *Server) initTus() {
	var (
		err     error
		fileLog *os.File
		bigDir  string
	)
	BIG_DIR := STORE_DIR + "/_big/" + common.Config().PeerId
	os.MkdirAll(BIG_DIR, 0775)
	os.MkdirAll(LOG_DIR, 0775)
	store := filestore.FileStore{
		Path: BIG_DIR,
	}
	if fileLog, err = os.OpenFile(LOG_DIR+"/tusd.log", os.O_CREATE|os.O_RDWR, 0666); err != nil {
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
					common.Util.CopyFile(LOG_DIR+"/tusd.log", LOG_DIR+"/tusd.log.2")
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
	if common.Config().SupportGroupManage {
		bigDir = fmt.Sprintf("/%s%s", common.Config().Group, cont.CONST_BIG_UPLOAD_PATH_SUFFIX)
	}
	composer := tusd.NewStoreComposer()
	// support raw tus upload and download
	store.GetReaderExt = func(id string) (io.Reader, error) {
		var (
			offset int64
			err    error
			length int
			buffer []byte
			fi     *common.FileInfo
			fn     string
		)
		if fi, err = server.GetFileInfoFromLevelDB(id); err != nil {
			log.Error(err)
			return nil, err
		} else {
			if common.Config().AuthUrl != "" {
				fileResult := common.Util.JsonEncodePretty(server.BuildFileResult(fi, nil))
				bufferReader := bytes.NewBuffer([]byte(fileResult))
				return bufferReader, nil
			}
			fn = fi.Name
			if fi.ReName != "" {
				fn = fi.ReName
			}
			fp := DOCKER_DIR + fi.Path + "/" + fn
			if common.Util.FileExists(fp) {
				log.Info(fmt.Sprintf("download:%s", fp))
				return os.Open(fp)
			}
			ps := strings.Split(fp, ",")
			if len(ps) > 2 && common.Util.FileExists(ps[0]) {
				if length, err = strconv.Atoi(ps[2]); err != nil {
					return nil, err
				}
				if offset, err = strconv.ParseInt(ps[1], 10, 64); err != nil {
					return nil, err
				}
				if buffer, err = common.Util.ReadFileByOffSet(ps[0], offset, length); err != nil {
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
		composer.UseCore(web.HookDataStore{
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
				scene := common.Config().DefaultScene
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
				if md5sum, err = common.Util.GetFileSumByName(oldFullPath, common.Config().FileSumArithmetic); err != nil {
					log.Error(err)
					continue
				}
				ext := path.Ext(name)
				filename := md5sum + ext
				if name != "" {
					filename = name
				}
				if common.Config().RenameFile {
					filename = md5sum + ext
				}
				timeStamp := time.Now().Unix()
				fpath := time.Now().Format("/20060102/15/04/")
				if pathCustom != "" {
					fpath = "/" + strings.Replace(pathCustom, ".", "", -1) + "/"
				}
				newFullPath := STORE_DIR + "/" + scene + fpath + common.Config().PeerId + "/" + filename
				if pathCustom != "" {
					newFullPath = STORE_DIR + "/" + scene + fpath + filename
				}
				if fi, err := server.GetFileInfoFromLevelDB(md5sum); err != nil {
					log.Error(err)
				} else {
					tpath := server.GetFilePathByInfo(fi, true)
					if fi.Md5 != "" && common.Util.FileExists(tpath) {
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
				fpath = cont.STORE_DIR_NAME + "/" + common.Config().DefaultScene + fpath + common.Config().PeerId
				os.MkdirAll(DOCKER_DIR+fpath, 0775)
				fileInfo := &common.FileInfo{
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
				callBack := func(info tusd.FileInfo, fileInfo *common.FileInfo) {
					if callback_url, ok := info.MetaData["callback_url"]; ok {
						req := httplib.Post(callback_url)
						req.SetTimeout(time.Second*10, time.Second*10)
						req.Param("info", common.Util.JsonEncodePretty(fileInfo))
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
