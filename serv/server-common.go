package serv

import (
	"errors"
	"fmt"
	"github.com/astaxie/beego/httplib"
	mapset "github.com/deckarep/golang-set"
	_ "github.com/eventials/go-tus"
	"github.com/sjqzhang/googleAuthenticator"
	log "github.com/sjqzhang/seelog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/thinxz-yuan/go-fastdfs/common"
	"github.com/thinxz-yuan/go-fastdfs/serv/config"
	"github.com/thinxz-yuan/go-fastdfs/serv/cont"
	"io"
	"mime/multipart"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"
)

func (server *Server) BackUpMetaDataByDate(date string) {
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("BackUpMetaDataByDate")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	var (
		err          error
		keyPrefix    string
		msg          string
		name         string
		fileInfo     common.FileInfo
		logFileName  string
		fileLog      *os.File
		fileMeta     *os.File
		metaFileName string
		fi           os.FileInfo
	)
	logFileName = DATA_DIR + "/" + date + "/" + cont.CONST_FILE_Md5_FILE_NAME
	server.lockMap.LockKey(logFileName)
	defer server.lockMap.UnLockKey(logFileName)
	metaFileName = DATA_DIR + "/" + date + "/" + "meta.data"
	os.MkdirAll(DATA_DIR+"/"+date, 0775)
	if server.util.IsExist(logFileName) {
		os.Remove(logFileName)
	}
	if server.util.IsExist(metaFileName) {
		os.Remove(metaFileName)
	}
	fileLog, err = os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
		log.Error(err)
		return
	}
	defer fileLog.Close()
	fileMeta, err = os.OpenFile(metaFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
		log.Error(err)
		return
	}
	defer fileMeta.Close()
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, cont.CONST_FILE_Md5_FILE_NAME)
	iter := server.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter.Release()
	for iter.Next() {
		if err = json.Unmarshal(iter.Value(), &fileInfo); err != nil {
			continue
		}
		name = fileInfo.Name
		if fileInfo.ReName != "" {
			name = fileInfo.ReName
		}
		msg = fmt.Sprintf("%s\t%s\n", fileInfo.Md5, string(iter.Value()))
		if _, err = fileMeta.WriteString(msg); err != nil {
			log.Error(err)
		}
		msg = fmt.Sprintf("%s\t%s\n", server.util.MD5(fileInfo.Path+"/"+name), string(iter.Value()))
		if _, err = fileMeta.WriteString(msg); err != nil {
			log.Error(err)
		}
		msg = fmt.Sprintf("%s|%d|%d|%s\n", fileInfo.Md5, fileInfo.Size, fileInfo.TimeStamp, fileInfo.Path+"/"+name)
		if _, err = fileLog.WriteString(msg); err != nil {
			log.Error(err)
		}
	}
	if fi, err = fileLog.Stat(); err != nil {
		log.Error(err)
	} else if fi.Size() == 0 {
		fileLog.Close()
		os.Remove(logFileName)
	}
	if fi, err = fileMeta.Stat(); err != nil {
		log.Error(err)
	} else if fi.Size() == 0 {
		fileMeta.Close()
		os.Remove(metaFileName)
	}
}

func (server *Server) SaveFileInfoToLevelDB(key string, fileInfo *common.FileInfo, db *leveldb.DB) (*common.FileInfo, error) {
	var (
		err  error
		data []byte
	)
	if fileInfo == nil || db == nil {
		return nil, errors.New("fileInfo is null or db is null")
	}
	if data, err = json.Marshal(fileInfo); err != nil {
		return fileInfo, err
	}
	if err = db.Put([]byte(key), data, nil); err != nil {
		return fileInfo, err
	}
	if db == server.ldb { //search slow ,write fast, double write logDB
		logDate := server.util.GetDayFromTimeStamp(fileInfo.TimeStamp)
		logKey := fmt.Sprintf("%s_%s_%s", logDate, cont.CONST_FILE_Md5_FILE_NAME, fileInfo.Md5)
		server.logDB.Put([]byte(logKey), data, nil)
	}
	return fileInfo, nil
}
func (server *Server) DownloadFromPeer(peer string, fileInfo *common.FileInfo) {
	var (
		err         error
		filename    string
		fpath       string
		fpathTmp    string
		fi          os.FileInfo
		sum         string
		data        []byte
		downloadUrl string
	)
	if config.Config().ReadOnly {
		log.Warn("ReadOnly", fileInfo)
		return
	}
	if config.Config().RetryCount > 0 && fileInfo.Retry >= config.Config().RetryCount {
		log.Error("DownloadFromPeer Error ", fileInfo)
		return
	} else {
		fileInfo.Retry = fileInfo.Retry + 1
	}
	filename = fileInfo.Name
	if fileInfo.ReName != "" {
		filename = fileInfo.ReName
	}
	if fileInfo.OffSet != -2 && config.Config().EnableDistinctFile && server.CheckFileExistByInfo(fileInfo.Md5, fileInfo) {
		// ignore migrate file
		log.Info(fmt.Sprintf("DownloadFromPeer file Exist, path:%s", fileInfo.Path+"/"+fileInfo.Name))
		return
	}
	if (!config.Config().EnableDistinctFile || fileInfo.OffSet == -2) && server.util.FileExists(server.GetFilePathByInfo(fileInfo, true)) {
		// ignore migrate file
		if fi, err = os.Stat(server.GetFilePathByInfo(fileInfo, true)); err == nil {
			if fi.ModTime().Unix() > fileInfo.TimeStamp {
				log.Info(fmt.Sprintf("ignore file sync path:%s", server.GetFilePathByInfo(fileInfo, false)))
				fileInfo.TimeStamp = fi.ModTime().Unix()
				server.postFileToPeer(fileInfo) // keep newer
				return
			}
			os.Remove(server.GetFilePathByInfo(fileInfo, true))
		}
	}
	if _, err = os.Stat(fileInfo.Path); err != nil {
		os.MkdirAll(DOCKER_DIR+fileInfo.Path, 0775)
	}
	//fmt.Println("downloadFromPeer",fileInfo)
	p := strings.Replace(fileInfo.Path, cont.STORE_DIR_NAME+"/", "", 1)
	//filename=server.util.UrlEncode(filename)
	downloadUrl = peer + "/" + config.Config().Group + "/" + p + "/" + filename
	log.Info("DownloadFromPeer: ", downloadUrl)
	fpath = DOCKER_DIR + fileInfo.Path + "/" + filename
	fpathTmp = DOCKER_DIR + fileInfo.Path + "/" + fmt.Sprintf("%s_%s", "tmp_", filename)
	timeout := fileInfo.Size/1024/1024/1 + 30
	if config.Config().SyncTimeout > 0 {
		timeout = config.Config().SyncTimeout
	}
	server.lockMap.LockKey(fpath)
	defer server.lockMap.UnLockKey(fpath)
	download_key := fmt.Sprintf("downloading_%d_%s", time.Now().Unix(), fpath)
	server.ldb.Put([]byte(download_key), []byte(""), nil)
	defer func() {
		server.ldb.Delete([]byte(download_key), nil)
	}()
	if fileInfo.OffSet == -2 {
		//migrate file
		if fi, err = os.Stat(fpath); err == nil && fi.Size() == fileInfo.Size {
			//prevent double download
			server.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, server.ldb)
			//log.Info(fmt.Sprintf("file '%s' has download", fpath))
			return
		}
		req := httplib.Get(downloadUrl)
		req.SetTimeout(time.Second*30, time.Second*time.Duration(timeout))
		if err = req.ToFile(fpathTmp); err != nil {
			server.AppendToDownloadQueue(fileInfo) //retry
			os.Remove(fpathTmp)
			log.Error(err, fpathTmp)
			return
		}
		if os.Rename(fpathTmp, fpath) == nil {
			//server.SaveFileMd5Log(fileInfo, CONST_FILE_Md5_FILE_NAME)
			server.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, server.ldb)
		}
		return
	}
	req := httplib.Get(downloadUrl)
	req.SetTimeout(time.Second*30, time.Second*time.Duration(timeout))
	if fileInfo.OffSet >= 0 {
		//small file download
		data, err = req.Bytes()
		if err != nil {
			server.AppendToDownloadQueue(fileInfo) //retry
			log.Error(err)
			return
		}
		data2 := make([]byte, len(data)+1)
		data2[0] = '1'
		for i, v := range data {
			data2[i+1] = v
		}
		data = data2
		if int64(len(data)) != fileInfo.Size {
			log.Warn("file size is error")
			return
		}
		fpath = strings.Split(fpath, ",")[0]
		err = server.util.WriteFileByOffSet(fpath, fileInfo.OffSet, data)
		if err != nil {
			log.Warn(err)
			return
		}
		server.SaveFileMd5Log(fileInfo, cont.CONST_FILE_Md5_FILE_NAME)
		return
	}
	if err = req.ToFile(fpathTmp); err != nil {
		server.AppendToDownloadQueue(fileInfo) //retry
		os.Remove(fpathTmp)
		log.Error(err)
		return
	}
	if fi, err = os.Stat(fpathTmp); err != nil {
		os.Remove(fpathTmp)
		return
	}
	_ = sum
	//if Config().EnableDistinctFile {
	//	//DistinctFile
	//	if sum, err = server.util.GetFileSumByName(fpathTmp, Config().FileSumArithmetic); err != nil {
	//		log.Error(err)
	//		return
	//	}
	//} else {
	//	//DistinctFile By path
	//	sum = server.util.MD5(server.GetFilePathByInfo(fileInfo, false))
	//}
	if fi.Size() != fileInfo.Size { //  maybe has bug remove || sum != fileInfo.Md5
		log.Error("file sum check error")
		os.Remove(fpathTmp)
		return
	}
	if os.Rename(fpathTmp, fpath) == nil {
		server.SaveFileMd5Log(fileInfo, cont.CONST_FILE_Md5_FILE_NAME)
	}
}
func (server *Server) SaveFileMd5Log(fileInfo *common.FileInfo, filename string) {
	var (
		info common.FileInfo
	)
	for len(server.queueFileLog)+len(server.queueFileLog)/10 > CONST_QUEUE_SIZE {
		time.Sleep(time.Second * 1)
	}
	info = *fileInfo
	server.queueFileLog <- &common.FileLog{FileInfo: &info, FileName: filename}
}
func (server *Server) AppendToQueue(fileInfo *common.FileInfo) {

	for (len(server.queueToPeers) + CONST_QUEUE_SIZE/10) > CONST_QUEUE_SIZE {
		time.Sleep(time.Millisecond * 50)
	}
	server.queueToPeers <- *fileInfo
}
func (server *Server) AppendToDownloadQueue(fileInfo *common.FileInfo) {
	for (len(server.queueFromPeers) + CONST_QUEUE_SIZE/10) > CONST_QUEUE_SIZE {
		time.Sleep(time.Millisecond * 50)
	}
	server.queueFromPeers <- *fileInfo
}
func (server *Server) getRequestURI(action string) string {
	var (
		uri string
	)
	if config.Config().SupportGroupManage {
		uri = "/" + config.Config().Group + "/" + action
	} else {
		uri = "/" + action
	}
	return uri
}

func (server *Server) postFileToPeer(fileInfo *common.FileInfo) {
	var (
		err      error
		peer     string
		filename string
		info     *common.FileInfo
		postURL  string
		result   string
		fi       os.FileInfo
		i        int
		data     []byte
		fpath    string
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("postFileToPeer")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	//fmt.Println("postFile",fileInfo)
	for i, peer = range config.Config().Peers {
		_ = i
		if fileInfo.Peers == nil {
			fileInfo.Peers = []string{}
		}
		if server.util.Contains(peer, fileInfo.Peers) {
			continue
		}
		filename = fileInfo.Name
		if fileInfo.ReName != "" {
			filename = fileInfo.ReName
			if fileInfo.OffSet != -1 {
				filename = strings.Split(fileInfo.ReName, ",")[0]
			}
		}
		fpath = DOCKER_DIR + fileInfo.Path + "/" + filename
		if !server.util.FileExists(fpath) {
			log.Warn(fmt.Sprintf("file '%s' not found", fpath))
			continue
		} else {
			if fileInfo.Size == 0 {
				if fi, err = os.Stat(fpath); err != nil {
					log.Error(err)
				} else {
					fileInfo.Size = fi.Size()
				}
			}
		}
		if fileInfo.OffSet != -2 && config.Config().EnableDistinctFile {
			//not migrate file should check or update file
			// where not EnableDistinctFile should check
			if info, err = server.checkPeerFileExist(peer, fileInfo.Md5, ""); info.Md5 != "" {
				fileInfo.Peers = append(fileInfo.Peers, peer)
				if _, err = server.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, server.ldb); err != nil {
					log.Error(err)
				}
				continue
			}
		}
		postURL = fmt.Sprintf("%s%s", peer, server.getRequestURI("syncfile_info"))
		b := httplib.Post(postURL)
		b.SetTimeout(time.Second*30, time.Second*30)
		if data, err = json.Marshal(fileInfo); err != nil {
			log.Error(err)
			return
		}
		b.Param("fileInfo", string(data))
		result, err = b.String()
		if err != nil {
			if fileInfo.Retry <= config.Config().RetryCount {
				fileInfo.Retry = fileInfo.Retry + 1
				server.AppendToQueue(fileInfo)
			}
			log.Error(err, fmt.Sprintf(" path:%s", fileInfo.Path+"/"+fileInfo.Name))
		}
		if !strings.HasPrefix(result, "http://") || err != nil {
			server.SaveFileMd5Log(fileInfo, cont.CONST_Md5_ERROR_FILE_NAME)
		}
		if strings.HasPrefix(result, "http://") {
			log.Info(result)
			if !server.util.Contains(peer, fileInfo.Peers) {
				fileInfo.Peers = append(fileInfo.Peers, peer)
				if _, err = server.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, server.ldb); err != nil {
					log.Error(err)
				}
			}
		}
		if err != nil {
			log.Error(err)
		}
	}
}
func (server *Server) saveFileMd5Log(fileInfo *common.FileInfo, filename string) {
	var (
		err      error
		outname  string
		logDate  string
		ok       bool
		fullpath string
		md5Path  string
		logKey   string
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("saveFileMd5Log")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	if fileInfo == nil || fileInfo.Md5 == "" || filename == "" {
		log.Warn("saveFileMd5Log", fileInfo, filename)
		return
	}
	logDate = server.util.GetDayFromTimeStamp(fileInfo.TimeStamp)
	outname = fileInfo.Name
	if fileInfo.ReName != "" {
		outname = fileInfo.ReName
	}
	fullpath = fileInfo.Path + "/" + outname
	logKey = fmt.Sprintf("%s_%s_%s", logDate, filename, fileInfo.Md5)
	if filename == cont.CONST_FILE_Md5_FILE_NAME {
		//server.searchMap.Put(fileInfo.Md5, fileInfo.Name)
		if ok, err = server.IsExistFromLevelDB(fileInfo.Md5, server.ldb); !ok {
			server.statMap.AddCountInt64(logDate+"_"+cont.CONST_STAT_FILE_COUNT_KEY, 1)
			server.statMap.AddCountInt64(logDate+"_"+cont.CONST_STAT_FILE_TOTAL_SIZE_KEY, fileInfo.Size)
			server.SaveStat()
		}
		if _, err = server.SaveFileInfoToLevelDB(logKey, fileInfo, server.logDB); err != nil {
			log.Error(err)
		}
		if _, err := server.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, server.ldb); err != nil {
			log.Error("saveToLevelDB", err, fileInfo)
		}
		if _, err = server.SaveFileInfoToLevelDB(server.util.MD5(fullpath), fileInfo, server.ldb); err != nil {
			log.Error("saveToLevelDB", err, fileInfo)
		}
		return
	}
	if filename == cont.CONST_REMOME_Md5_FILE_NAME {
		//server.searchMap.Remove(fileInfo.Md5)
		if ok, err = server.IsExistFromLevelDB(fileInfo.Md5, server.ldb); ok {
			server.statMap.AddCountInt64(logDate+"_"+cont.CONST_STAT_FILE_COUNT_KEY, -1)
			server.statMap.AddCountInt64(logDate+"_"+cont.CONST_STAT_FILE_TOTAL_SIZE_KEY, -fileInfo.Size)
			server.SaveStat()
		}
		server.RemoveKeyFromLevelDB(logKey, server.logDB)
		md5Path = server.util.MD5(fullpath)
		if err := server.RemoveKeyFromLevelDB(fileInfo.Md5, server.ldb); err != nil {
			log.Error("RemoveKeyFromLevelDB", err, fileInfo)
		}
		if err = server.RemoveKeyFromLevelDB(md5Path, server.ldb); err != nil {
			log.Error("RemoveKeyFromLevelDB", err, fileInfo)
		}
		// remove files.md5 for stat info(repair from logDB)
		logKey = fmt.Sprintf("%s_%s_%s", logDate, cont.CONST_FILE_Md5_FILE_NAME, fileInfo.Md5)
		server.RemoveKeyFromLevelDB(logKey, server.logDB)
		return
	}
	server.SaveFileInfoToLevelDB(logKey, fileInfo, server.logDB)
}
func (server *Server) BuildFileResult(fileInfo *common.FileInfo, r *http.Request) common.FileResult {
	var (
		outname     string
		fileResult  common.FileResult
		p           string
		downloadUrl string
		domain      string
		host        string
	)
	host = strings.Replace(config.Config().Host, "http://", "", -1)
	if r != nil {
		host = r.Host
	}
	if !strings.HasPrefix(config.Config().DownloadDomain, "http") {
		if config.Config().DownloadDomain == "" {
			config.Config().DownloadDomain = fmt.Sprintf("http://%s", host)
		} else {
			config.Config().DownloadDomain = fmt.Sprintf("http://%s", config.Config().DownloadDomain)
		}
	}
	if config.Config().DownloadDomain != "" {
		domain = config.Config().DownloadDomain
	} else {
		domain = fmt.Sprintf("http://%s", host)
	}
	outname = fileInfo.Name
	if fileInfo.ReName != "" {
		outname = fileInfo.ReName
	}
	p = strings.Replace(fileInfo.Path, cont.STORE_DIR_NAME+"/", "", 1)
	if config.Config().SupportGroupManage {
		p = config.Config().Group + "/" + p + "/" + outname
	} else {
		p = p + "/" + outname
	}
	downloadUrl = fmt.Sprintf("http://%s/%s", host, p)
	if config.Config().DownloadDomain != "" {
		downloadUrl = fmt.Sprintf("%s/%s", config.Config().DownloadDomain, p)
	}
	fileResult.Url = downloadUrl
	fileResult.Md5 = fileInfo.Md5
	fileResult.Path = "/" + p
	fileResult.Domain = domain
	fileResult.Scene = fileInfo.Scene
	fileResult.Size = fileInfo.Size
	fileResult.ModTime = fileInfo.TimeStamp
	// Just for Compatibility
	fileResult.Src = fileResult.Path
	fileResult.Scenes = fileInfo.Scene
	return fileResult
}
func (server *Server) GetFilePathByInfo(fileInfo *common.FileInfo, withDocker bool) string {
	var (
		fn string
	)
	fn = fileInfo.Name
	if fileInfo.ReName != "" {
		fn = fileInfo.ReName
	}
	if withDocker {
		return DOCKER_DIR + fileInfo.Path + "/" + fn
	}
	return fileInfo.Path + "/" + fn
}

func (server *Server) checkPeerFileExist(peer string, md5sum string, fpath string) (*common.FileInfo, error) {
	var (
		err      error
		fileInfo common.FileInfo
	)
	req := httplib.Post(fmt.Sprintf("%s%s?md5=%s", peer, server.getRequestURI("check_file_exist"), md5sum))
	req.Param("path", fpath)
	req.Param("md5", md5sum)
	req.SetTimeout(time.Second*5, time.Second*10)
	if err = req.ToJSON(&fileInfo); err != nil {
		return &common.FileInfo{}, err
	}
	if fileInfo.Md5 == "" {
		return &fileInfo, errors.New("not found")
	}
	return &fileInfo, nil
}
func (server *Server) RemoveKeyFromLevelDB(key string, db *leveldb.DB) error {
	var (
		err error
	)
	err = db.Delete([]byte(key), nil)
	return err
}
func (server *Server) GetMd5sByDate(date string, filename string) (mapset.Set, error) {
	var (
		keyPrefix string
		md5set    mapset.Set
		keys      []string
	)
	md5set = mapset.NewSet()
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, filename)
	iter := server.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	for iter.Next() {
		keys = strings.Split(string(iter.Key()), "_")
		if len(keys) >= 3 {
			md5set.Add(keys[2])
		}
	}
	iter.Release()
	return md5set, nil
}
func (server *Server) NotPermit(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(401)
}
func (server *Server) CheckAuth(w http.ResponseWriter, r *http.Request) bool {
	var (
		err        error
		req        *httplib.BeegoHTTPRequest
		result     string
		jsonResult common.JsonResult
	)
	if err = r.ParseForm(); err != nil {
		log.Error(err)
		return false
	}
	req = httplib.Post(config.Config().AuthUrl)
	req.SetTimeout(time.Second*10, time.Second*10)
	req.Param("__path__", r.URL.Path)
	req.Param("__query__", r.URL.RawQuery)
	for k, _ := range r.Form {
		req.Param(k, r.FormValue(k))
	}
	for k, v := range r.Header {
		req.Header(k, v[0])
	}
	result, err = req.String()
	result = strings.TrimSpace(result)
	if strings.HasPrefix(result, "{") && strings.HasSuffix(result, "}") {
		if err = json.Unmarshal([]byte(result), &jsonResult); err != nil {
			log.Error(err)
			return false
		}
		if jsonResult.Data != "ok" {
			log.Warn(result)
			return false
		}
	} else {
		if result != "ok" {
			log.Warn(result)
			return false
		}
	}
	return true
}
func (server *Server) VerifyGoogleCode(secret string, code string, discrepancy int64) bool {
	var (
		goauth *googleAuthenticator.GAuth
	)
	goauth = googleAuthenticator.NewGAuth()
	if ok, err := goauth.VerifyCode(secret, code, discrepancy); ok {
		return ok
	} else {
		log.Error(err)
		return ok
	}
}

func (server *Server) upload(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ok  bool
		//		pathname     string
		md5sum       string
		fileName     string
		fileInfo     common.FileInfo
		uploadFile   multipart.File
		uploadHeader *multipart.FileHeader
		scene        string
		output       string
		fileResult   common.FileResult
		data         []byte
		code         string
		secret       interface{}
	)
	output = r.FormValue("output")
	if config.Config().EnableCrossOrigin {
		server.CrossOrigin(w, r)
		if r.Method == http.MethodOptions {
			return
		}
	}

	if config.Config().AuthUrl != "" {
		if !server.CheckAuth(w, r) {
			log.Warn("auth fail", r.Form)
			server.NotPermit(w, r)
			w.Write([]byte("auth fail"))
			return
		}
	}
	if r.Method == http.MethodPost {
		md5sum = r.FormValue("md5")
		fileName = r.FormValue("filename")
		output = r.FormValue("output")
		if config.Config().ReadOnly {
			w.Write([]byte("(error) readonly"))
			return
		}
		if config.Config().EnableCustomPath {
			fileInfo.Path = r.FormValue("path")
			fileInfo.Path = strings.Trim(fileInfo.Path, "/")
		}
		scene = r.FormValue("scene")
		code = r.FormValue("code")
		if scene == "" {
			//Just for Compatibility
			scene = r.FormValue("scenes")
		}
		if config.Config().EnableGoogleAuth && scene != "" {
			if secret, ok = server.sceneMap.GetValue(scene); ok {
				if !server.VerifyGoogleCode(secret.(string), code, int64(config.Config().DownloadTokenExpire/30)) {
					server.NotPermit(w, r)
					w.Write([]byte("invalid request,error google code"))
					return
				}
			}
		}
		fileInfo.Md5 = md5sum
		fileInfo.ReName = fileName
		fileInfo.OffSet = -1
		if uploadFile, uploadHeader, err = r.FormFile("file"); err != nil {
			log.Error(err)
			w.Write([]byte(err.Error()))
			return
		}
		fileInfo.Peers = []string{}
		fileInfo.TimeStamp = time.Now().Unix()
		if scene == "" {
			scene = config.Config().DefaultScene
		}
		if output == "" {
			output = "text"
		}
		if !server.util.Contains(output, []string{"json", "text"}) {
			w.Write([]byte("output just support json or text"))
			return
		}
		fileInfo.Scene = scene
		if _, err = server.checkScene(scene); err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		if err != nil {
			log.Error(err)
			http.Redirect(w, r, "/", http.StatusMovedPermanently)
			return
		}
		if _, err = server.saveUploadFile(uploadFile, uploadHeader, &fileInfo, r); err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		if config.Config().EnableDistinctFile {
			if v, _ := server.GetFileInfoFromLevelDB(fileInfo.Md5); v != nil && v.Md5 != "" {
				fileResult = server.BuildFileResult(v, r)
				if config.Config().RenameFile {
					os.Remove(DOCKER_DIR + fileInfo.Path + "/" + fileInfo.ReName)
				} else {
					os.Remove(DOCKER_DIR + fileInfo.Path + "/" + fileInfo.Name)
				}
				if output == "json" {
					if data, err = json.Marshal(fileResult); err != nil {
						log.Error(err)
						w.Write([]byte(err.Error()))
					}
					w.Write(data)
				} else {
					w.Write([]byte(fileResult.Url))
				}
				return
			}
		}
		if fileInfo.Md5 == "" {
			log.Warn(" fileInfo.Md5 is null")
			return
		}
		if md5sum != "" && fileInfo.Md5 != md5sum {
			log.Warn(" fileInfo.Md5 and md5sum !=")
			return
		}
		if !config.Config().EnableDistinctFile {
			// bugfix filecount stat
			fileInfo.Md5 = server.util.MD5(server.GetFilePathByInfo(&fileInfo, false))
		}
		if config.Config().EnableMergeSmallFile && fileInfo.Size < cont.CONST_SMALL_FILE_SIZE {
			if err = server.saveSmallFile(&fileInfo); err != nil {
				log.Error(err)
				return
			}
		}
		server.saveFileMd5Log(&fileInfo, cont.CONST_FILE_Md5_FILE_NAME) //maybe slow
		go server.postFileToPeer(&fileInfo)
		if fileInfo.Size <= 0 {
			log.Error("file size is zero")
			return
		}
		fileResult = server.BuildFileResult(&fileInfo, r)
		if output == "json" {
			if data, err = json.Marshal(fileResult); err != nil {
				log.Error(err)
				w.Write([]byte(err.Error()))
			}
			w.Write(data)
		} else {
			w.Write([]byte(fileResult.Url))
		}
		return
	} else {
		md5sum = r.FormValue("md5")
		output = r.FormValue("output")
		if md5sum == "" {
			w.Write([]byte("(error) if you want to upload fast md5 is require" +
				",and if you want to upload file,you must use post method  "))
			return
		}
		if v, _ := server.GetFileInfoFromLevelDB(md5sum); v != nil && v.Md5 != "" {
			fileResult = server.BuildFileResult(v, r)
		}
		if output == "json" {
			if data, err = json.Marshal(fileResult); err != nil {
				log.Error(err)
				w.Write([]byte(err.Error()))
			}
			w.Write(data)
		} else {
			w.Write([]byte(fileResult.Url))
		}
	}
}
func (server *Server) saveUploadFile(file multipart.File, header *multipart.FileHeader, fileInfo *common.FileInfo, r *http.Request) (*common.FileInfo, error) {
	var (
		err     error
		outFile *os.File
		folder  string
		fi      os.FileInfo
	)
	defer file.Close()
	_, fileInfo.Name = filepath.Split(header.Filename)
	// bugfix for ie upload file contain fullpath
	if len(config.Config().Extensions) > 0 && !server.util.Contains(path.Ext(fileInfo.Name), config.Config().Extensions) {
		return fileInfo, errors.New("(error)file extension mismatch")
	}

	if config.Config().RenameFile {
		fileInfo.ReName = server.util.MD5(server.util.GetUUID()) + path.Ext(fileInfo.Name)
	}
	folder = time.Now().Format("20060102/15/04")
	if config.Config().PeerId != "" {
		folder = fmt.Sprintf(folder+"/%s", config.Config().PeerId)
	}
	if fileInfo.Scene != "" {
		folder = fmt.Sprintf(STORE_DIR+"/%s/%s", fileInfo.Scene, folder)
	} else {
		folder = fmt.Sprintf(STORE_DIR+"/%s", folder)
	}
	if fileInfo.Path != "" {
		if strings.HasPrefix(fileInfo.Path, STORE_DIR) {
			folder = fileInfo.Path
		} else {
			folder = STORE_DIR + "/" + fileInfo.Path
		}
	}
	if !server.util.FileExists(folder) {
		os.MkdirAll(folder, 0775)
	}
	outPath := fmt.Sprintf(folder+"/%s", fileInfo.Name)
	if fileInfo.ReName != "" {
		outPath = fmt.Sprintf(folder+"/%s", fileInfo.ReName)
	}
	if server.util.FileExists(outPath) && config.Config().EnableDistinctFile {
		for i := 0; i < 10000; i++ {
			outPath = fmt.Sprintf(folder+"/%d_%s", i, filepath.Base(header.Filename))
			fileInfo.Name = fmt.Sprintf("%d_%s", i, header.Filename)
			if !server.util.FileExists(outPath) {
				break
			}
		}
	}
	log.Info(fmt.Sprintf("upload: %s", outPath))
	if outFile, err = os.Create(outPath); err != nil {
		return fileInfo, err
	}
	defer outFile.Close()
	if err != nil {
		log.Error(err)
		return fileInfo, errors.New("(error)fail," + err.Error())
	}
	if _, err = io.Copy(outFile, file); err != nil {
		log.Error(err)
		return fileInfo, errors.New("(error)fail," + err.Error())
	}
	if fi, err = outFile.Stat(); err != nil {
		log.Error(err)
	} else {
		fileInfo.Size = fi.Size()
	}
	if fi.Size() != header.Size {
		return fileInfo, errors.New("(error)file uncomplete")
	}
	v := "" // server.util.GetFileSum(outFile, Config().FileSumArithmetic)
	if config.Config().EnableDistinctFile {
		v = server.util.GetFileSum(outFile, config.Config().FileSumArithmetic)
	} else {
		v = server.util.MD5(server.GetFilePathByInfo(fileInfo, false))
	}
	fileInfo.Md5 = v
	//fileInfo.Path = folder //strings.Replace( folder,DOCKER_DIR,"",1)
	fileInfo.Path = strings.Replace(folder, DOCKER_DIR, "", 1)
	fileInfo.Peers = append(fileInfo.Peers, server.host)
	//fmt.Println("upload",fileInfo)
	return fileInfo, nil
}
func (server *Server) checkScene(scene string) (bool, error) {
	var (
		scenes []string
	)
	if len(config.Config().Scenes) == 0 {
		return true, nil
	}
	for _, s := range config.Config().Scenes {
		scenes = append(scenes, strings.Split(s, ":")[0])
	}
	if !server.util.Contains(scene, scenes) {
		return false, errors.New("not valid scene")
	}
	return true, nil
}
func (server *Server) saveSmallFile(fileInfo *common.FileInfo) error {
	var (
		err      error
		filename string
		fpath    string
		srcFile  *os.File
		desFile  *os.File
		largeDir string
		destPath string
		reName   string
		fileExt  string
	)
	filename = fileInfo.Name
	fileExt = path.Ext(filename)
	if fileInfo.ReName != "" {
		filename = fileInfo.ReName
	}
	fpath = DOCKER_DIR + fileInfo.Path + "/" + filename
	largeDir = LARGE_DIR + "/" + config.Config().PeerId
	if !server.util.FileExists(largeDir) {
		os.MkdirAll(largeDir, 0775)
	}
	reName = fmt.Sprintf("%d", server.util.RandInt(100, 300))
	destPath = largeDir + "/" + reName
	server.lockMap.LockKey(destPath)
	defer server.lockMap.UnLockKey(destPath)
	if server.util.FileExists(fpath) {
		srcFile, err = os.OpenFile(fpath, os.O_CREATE|os.O_RDONLY, 06666)
		if err != nil {
			return err
		}
		defer srcFile.Close()
		desFile, err = os.OpenFile(destPath, os.O_CREATE|os.O_RDWR, 06666)
		if err != nil {
			return err
		}
		defer desFile.Close()
		fileInfo.OffSet, err = desFile.Seek(0, 2)
		if _, err = desFile.Write([]byte("1")); err != nil {
			//first byte set 1
			return err
		}
		fileInfo.OffSet, err = desFile.Seek(0, 2)
		if err != nil {
			return err
		}
		fileInfo.OffSet = fileInfo.OffSet - 1 //minus 1 byte
		fileInfo.Size = fileInfo.Size + 1
		fileInfo.ReName = fmt.Sprintf("%s,%d,%d,%s", reName, fileInfo.OffSet, fileInfo.Size, fileExt)
		if _, err = io.Copy(desFile, srcFile); err != nil {
			return err
		}
		srcFile.Close()
		os.Remove(fpath)
		fileInfo.Path = strings.Replace(largeDir, DOCKER_DIR, "", 1)
	}
	return nil
}
