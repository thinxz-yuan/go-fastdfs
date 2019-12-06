package serv

import (
	"fmt"
	"github.com/astaxie/beego/httplib"
	mapset "github.com/deckarep/golang-set"
	_ "github.com/eventials/go-tus"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	"github.com/sjqzhang/googleAuthenticator"
	log "github.com/sjqzhang/seelog"
	"github.com/thinxz-yuan/go-fastdfs/serv/cont"
	"github.com/thinxz-yuan/go-fastdfs/serv/ent"
	"io"
	"io/ioutil"
	random "math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

func (server *Server) CheckFilesExist(w http.ResponseWriter, r *http.Request) {
	var (
		data      []byte
		err       error
		fileInfo  *ent.FileInfo
		fileInfos []*ent.FileInfo
		fpath     string
		result    ent.JsonResult
	)
	r.ParseForm()
	md5sum := ""
	md5sum = r.FormValue("md5s")
	md5s := strings.Split(md5sum, ",")
	for _, m := range md5s {
		if fileInfo, err = server.GetFileInfoFromLevelDB(m); fileInfo != nil {
			if fileInfo.OffSet != -1 {
				if data, err = json.Marshal(fileInfo); err != nil {
					log.Error(err)
				}
				//w.Write(data)
				//return
				fileInfos = append(fileInfos, fileInfo)
				continue
			}
			fpath = cont.DOCKER_DIR + fileInfo.Path + "/" + fileInfo.Name
			if fileInfo.ReName != "" {
				fpath = cont.DOCKER_DIR + fileInfo.Path + "/" + fileInfo.ReName
			}
			if server.util.IsExist(fpath) {
				if data, err = json.Marshal(fileInfo); err == nil {
					fileInfos = append(fileInfos, fileInfo)
					//w.Write(data)
					//return
					continue
				} else {
					log.Error(err)
				}
			} else {
				if fileInfo.OffSet == -1 {
					server.RemoveKeyFromLevelDB(md5sum, server.ldb) // when file delete,delete from leveldb
				}
			}
		}
	}
	result.Data = fileInfos
	data, _ = json.Marshal(result)
	w.Write(data)
	return
}

func (server *Server) CheckFileExist(w http.ResponseWriter, r *http.Request) {
	var (
		data     []byte
		err      error
		fileInfo *ent.FileInfo
		fpath    string
		fi       os.FileInfo
	)
	r.ParseForm()
	md5sum := ""
	md5sum = r.FormValue("md5")
	fpath = r.FormValue("path")
	if fileInfo, err = server.GetFileInfoFromLevelDB(md5sum); fileInfo != nil {
		if fileInfo.OffSet != -1 {
			if data, err = json.Marshal(fileInfo); err != nil {
				log.Error(err)
			}
			w.Write(data)
			return
		}
		fpath = cont.DOCKER_DIR + fileInfo.Path + "/" + fileInfo.Name
		if fileInfo.ReName != "" {
			fpath = cont.DOCKER_DIR + fileInfo.Path + "/" + fileInfo.ReName
		}
		if server.util.IsExist(fpath) {
			if data, err = json.Marshal(fileInfo); err == nil {
				w.Write(data)
				return
			} else {
				log.Error(err)
			}
		} else {
			if fileInfo.OffSet == -1 {
				server.RemoveKeyFromLevelDB(md5sum, server.ldb) // when file delete,delete from leveldb
			}
		}
	} else {
		if fpath != "" {
			fi, err = os.Stat(fpath)
			if err == nil {
				sum := server.util.MD5(fpath)
				//if Config().EnableDistinctFile {
				//	sum, err = server.util.GetFileSumByName(fpath, Config().FileSumArithmetic)
				//	if err != nil {
				//		log.Error(err)
				//	}
				//}
				fileInfo = &ent.FileInfo{
					Path:      path.Dir(fpath),
					Name:      path.Base(fpath),
					Size:      fi.Size(),
					Md5:       sum,
					Peers:     []string{Config().Host},
					OffSet:    -1, //very important
					TimeStamp: fi.ModTime().Unix(),
				}
				data, err = json.Marshal(fileInfo)
				w.Write(data)
				return
			}
		}
	}
	data, _ = json.Marshal(ent.FileInfo{})
	w.Write(data)
	return
}

func (server *Server) Upload(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		fn     string
		folder string
		fpTmp  *os.File
		fpBody *os.File
	)
	if r.Method == http.MethodGet {
		server.upload(w, r)
		return
	}
	folder = cont.STORE_DIR + "/_tmp/" + time.Now().Format("20060102")
	os.MkdirAll(folder, 0777)
	fn = folder + "/" + server.util.GetUUID()
	defer func() {
		os.Remove(fn)
	}()
	fpTmp, err = os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		log.Error(err)
		w.Write([]byte(err.Error()))
		return
	}
	defer fpTmp.Close()
	if _, err = io.Copy(fpTmp, r.Body); err != nil {
		log.Error(err)
		w.Write([]byte(err.Error()))
		return
	}
	fpBody, err = os.Open(fn)
	r.Body = fpBody
	done := make(chan bool, 1)
	server.queueUpload <- ent.WrapReqResp{&w, r, done}
	<-done

}

func (server *Server) RemoveFile(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		md5sum   string
		fileInfo *ent.FileInfo
		fpath    string
		delUrl   string
		result   ent.JsonResult
		inner    string
		name     string
	)
	_ = delUrl
	_ = inner
	r.ParseForm()
	md5sum = r.FormValue("md5")
	fpath = r.FormValue("path")
	inner = r.FormValue("inner")
	result.Status = "fail"
	if !server.IsPeer(r) {
		w.Write([]byte(server.GetClusterNotPermitMessage(r)))
		return
	}
	if Config().AuthUrl != "" && !server.CheckAuth(w, r) {
		server.NotPermit(w, r)
		return
	}
	if fpath != "" && md5sum == "" {
		fpath = strings.Replace(fpath, "/"+Config().Group+"/", cont.STORE_DIR_NAME+"/", 1)
		md5sum = server.util.MD5(fpath)
	}
	if inner != "1" {
		for _, peer := range Config().Peers {
			delFile := func(peer string, md5sum string, fileInfo *ent.FileInfo) {
				delUrl = fmt.Sprintf("%s%s", peer, server.getRequestURI("delete"))
				req := httplib.Post(delUrl)
				req.Param("md5", md5sum)
				req.Param("inner", "1")
				req.SetTimeout(time.Second*5, time.Second*10)
				if _, err = req.String(); err != nil {
					log.Error(err)
				}
			}
			go delFile(peer, md5sum, fileInfo)
		}
	}
	if len(md5sum) < 32 {
		result.Message = "md5 unvalid"
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	if fileInfo, err = server.GetFileInfoFromLevelDB(md5sum); err != nil {
		result.Message = err.Error()
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	if fileInfo.OffSet >= 0 {
		result.Message = "small file delete not support"
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	name = fileInfo.Name
	if fileInfo.ReName != "" {
		name = fileInfo.ReName
	}
	fpath = fileInfo.Path + "/" + name
	if fileInfo.Path != "" && server.util.FileExists(cont.DOCKER_DIR+fpath) {
		server.SaveFileMd5Log(fileInfo, cont.CONST_REMOME_Md5_FILE_NAME)
		if err = os.Remove(cont.DOCKER_DIR + fpath); err != nil {
			result.Message = err.Error()
			w.Write([]byte(server.util.JsonEncodePretty(result)))
			return
		} else {
			result.Message = "remove success"
			result.Status = "ok"
			w.Write([]byte(server.util.JsonEncodePretty(result)))
			return
		}
	}
	result.Message = "fail remove"
	w.Write([]byte(server.util.JsonEncodePretty(result)))
}

func (server *Server) GetFileInfo(w http.ResponseWriter, r *http.Request) {
	var (
		fpath    string
		md5sum   string
		fileInfo *ent.FileInfo
		err      error
		result   ent.JsonResult
	)
	md5sum = r.FormValue("md5")
	fpath = r.FormValue("path")
	result.Status = "fail"
	if !server.IsPeer(r) {
		w.Write([]byte(server.GetClusterNotPermitMessage(r)))
		return
	}
	md5sum = r.FormValue("md5")
	if fpath != "" {
		fpath = strings.Replace(fpath, "/"+Config().Group+"/", cont.STORE_DIR_NAME+"/", 1)
		md5sum = server.util.MD5(fpath)
	}
	if fileInfo, err = server.GetFileInfoFromLevelDB(md5sum); err != nil {
		log.Error(err)
		result.Message = err.Error()
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	result.Status = "ok"
	result.Data = fileInfo
	w.Write([]byte(server.util.JsonEncodePretty(result)))
	return
}

func (server *Server) Sync(w http.ResponseWriter, r *http.Request) {
	var (
		result ent.JsonResult
	)
	r.ParseForm()
	result.Status = "fail"
	if !server.IsPeer(r) {
		result.Message = "client must be in cluster"
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	date := ""
	force := ""
	inner := ""
	isForceUpload := false
	force = r.FormValue("force")
	date = r.FormValue("date")
	inner = r.FormValue("inner")
	if force == "1" {
		isForceUpload = true
	}
	if inner != "1" {
		for _, peer := range Config().Peers {
			req := httplib.Post(peer + server.getRequestURI("sync"))
			req.Param("force", force)
			req.Param("inner", "1")
			req.Param("date", date)
			if _, err := req.String(); err != nil {
				log.Error(err)
			}
		}
	}
	if date == "" {
		result.Message = "require paramete date &force , ?date=20181230"
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	date = strings.Replace(date, ".", "", -1)
	if isForceUpload {
		go server.CheckFileAndSendToPeer(date, cont.CONST_FILE_Md5_FILE_NAME, isForceUpload)
	} else {
		go server.CheckFileAndSendToPeer(date, cont.CONST_Md5_ERROR_FILE_NAME, isForceUpload)
	}
	result.Status = "ok"
	result.Message = "job is running"
	w.Write([]byte(server.util.JsonEncodePretty(result)))
}

func (server *Server) Stat(w http.ResponseWriter, r *http.Request) {
	var (
		result   ent.JsonResult
		inner    string
		echart   string
		category []string
		barCount []int64
		barSize  []int64
		dataMap  map[string]interface{}
	)
	if !server.IsPeer(r) {
		result.Message = server.GetClusterNotPermitMessage(r)
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	r.ParseForm()
	inner = r.FormValue("inner")
	echart = r.FormValue("echart")
	data := server.GetStat()
	result.Status = "ok"
	result.Data = data
	if echart == "1" {
		dataMap = make(map[string]interface{}, 3)
		for _, v := range data {
			barCount = append(barCount, v.FileCount)
			barSize = append(barSize, v.TotalSize)
			category = append(category, v.Date)
		}
		dataMap["category"] = category
		dataMap["barCount"] = barCount
		dataMap["barSize"] = barSize
		result.Data = dataMap
	}
	if inner == "1" {
		w.Write([]byte(server.util.JsonEncodePretty(data)))
	} else {
		w.Write([]byte(server.util.JsonEncodePretty(result)))
	}
}

func (server *Server) RepairStatWeb(w http.ResponseWriter, r *http.Request) {
	var (
		result ent.JsonResult
		date   string
		inner  string
	)
	if !server.IsPeer(r) {
		result.Message = server.GetClusterNotPermitMessage(r)
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	date = r.FormValue("date")
	inner = r.FormValue("inner")
	if ok, err := regexp.MatchString("\\d{8}", date); err != nil || !ok {
		result.Message = "invalid date"
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	if date == "" || len(date) != 8 {
		date = server.util.GetToDay()
	}
	if inner != "1" {
		for _, peer := range Config().Peers {
			req := httplib.Post(peer + server.getRequestURI("repair_stat"))
			req.Param("inner", "1")
			req.Param("date", date)
			if _, err := req.String(); err != nil {
				log.Error(err)
			}
		}
	}
	result.Data = server.RepairStatByDate(date)
	result.Status = "ok"
	w.Write([]byte(server.util.JsonEncodePretty(result)))
}

func (server *Server) Status(w http.ResponseWriter, r *http.Request) {
	var (
		status   ent.JsonResult
		sts      map[string]interface{}
		today    string
		sumset   mapset.Set
		ok       bool
		v        interface{}
		err      error
		appDir   string
		diskInfo *disk.UsageStat
		memInfo  *mem.VirtualMemoryStat
	)
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	today = server.util.GetToDay()
	sts = make(map[string]interface{})
	sts["Fs.QueueFromPeers"] = len(server.queueFromPeers)
	sts["Fs.QueueToPeers"] = len(server.queueToPeers)
	sts["Fs.QueueFileLog"] = len(server.queueFileLog)
	for _, k := range []string{cont.CONST_FILE_Md5_FILE_NAME, cont.CONST_Md5_ERROR_FILE_NAME, cont.CONST_Md5_QUEUE_FILE_NAME} {
		k2 := fmt.Sprintf("%s_%s", today, k)
		if v, ok = server.sumMap.GetValue(k2); ok {
			sumset = v.(mapset.Set)
			if k == cont.CONST_Md5_QUEUE_FILE_NAME {
				sts["Fs.QueueSetSize"] = sumset.Cardinality()
			}
			if k == cont.CONST_Md5_ERROR_FILE_NAME {
				sts["Fs.ErrorSetSize"] = sumset.Cardinality()
			}
			if k == cont.CONST_FILE_Md5_FILE_NAME {
				sts["Fs.FileSetSize"] = sumset.Cardinality()
			}
		}
	}
	sts["Fs.AutoRepair"] = Config().AutoRepair
	sts["Fs.QueueUpload"] = len(server.queueUpload)
	sts["Fs.RefreshInterval"] = Config().RefreshInterval
	sts["Fs.Peers"] = Config().Peers
	sts["Fs.Local"] = server.host
	sts["Fs.FileStats"] = server.GetStat()
	sts["Fs.ShowDir"] = Config().ShowDir
	sts["Sys.NumGoroutine"] = runtime.NumGoroutine()
	sts["Sys.NumCpu"] = runtime.NumCPU()
	sts["Sys.Alloc"] = memStat.Alloc
	sts["Sys.TotalAlloc"] = memStat.TotalAlloc
	sts["Sys.HeapAlloc"] = memStat.HeapAlloc
	sts["Sys.Frees"] = memStat.Frees
	sts["Sys.HeapObjects"] = memStat.HeapObjects
	sts["Sys.NumGC"] = memStat.NumGC
	sts["Sys.GCCPUFraction"] = memStat.GCCPUFraction
	sts["Sys.GCSys"] = memStat.GCSys
	//sts["Sys.MemInfo"] = memStat
	appDir, err = filepath.Abs(".")
	if err != nil {
		log.Error(err)
	}
	diskInfo, err = disk.Usage(appDir)
	if err != nil {
		log.Error(err)
	}
	sts["Sys.DiskInfo"] = diskInfo
	memInfo, err = mem.VirtualMemory()
	if err != nil {
		log.Error(err)
	}
	sts["Sys.MemInfo"] = memInfo
	status.Status = "ok"
	status.Data = sts
	w.Write([]byte(server.util.JsonEncodePretty(status)))
}

func (server *Server) Repair(w http.ResponseWriter, r *http.Request) {
	var (
		force       string
		forceRepair bool
		result      ent.JsonResult
	)
	result.Status = "ok"
	r.ParseForm()
	force = r.FormValue("force")
	if force == "1" {
		forceRepair = true
	}
	if server.IsPeer(r) {
		go server.AutoRepair(forceRepair)
		result.Message = "repair job start..."
		w.Write([]byte(server.util.JsonEncodePretty(result)))
	} else {
		result.Message = server.GetClusterNotPermitMessage(r)
		w.Write([]byte(server.util.JsonEncodePretty(result)))
	}

}

func (server *Server) Report(w http.ResponseWriter, r *http.Request) {
	var (
		reportFileName string
		result         ent.JsonResult
		html           string
	)
	result.Status = "ok"
	r.ParseForm()
	if server.IsPeer(r) {
		reportFileName = cont.STATIC_DIR + "/report.html"
		if server.util.IsExist(reportFileName) {
			if data, err := server.util.ReadBinFile(reportFileName); err != nil {
				log.Error(err)
				result.Message = err.Error()
				w.Write([]byte(server.util.JsonEncodePretty(result)))
				return
			} else {
				html = string(data)
				if Config().SupportGroupManage {
					html = strings.Replace(html, "{group}", "/"+Config().Group, 1)
				} else {
					html = strings.Replace(html, "{group}", "", 1)
				}
				w.Write([]byte(html))
				return
			}
		} else {
			w.Write([]byte(fmt.Sprintf("%s is not found", reportFileName)))
		}
	} else {
		w.Write([]byte(server.GetClusterNotPermitMessage(r)))
	}
}

func (server *Server) BackUp(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		date   string
		result ent.JsonResult
		inner  string
		url    string
	)
	result.Status = "ok"
	r.ParseForm()
	date = r.FormValue("date")
	inner = r.FormValue("inner")
	if date == "" {
		date = server.util.GetToDay()
	}
	if server.IsPeer(r) {
		if inner != "1" {
			for _, peer := range Config().Peers {
				backUp := func(peer string, date string) {
					url = fmt.Sprintf("%s%s", peer, server.getRequestURI("backup"))
					req := httplib.Post(url)
					req.Param("date", date)
					req.Param("inner", "1")
					req.SetTimeout(time.Second*5, time.Second*600)
					if _, err = req.String(); err != nil {
						log.Error(err)
					}
				}
				go backUp(peer, date)
			}
		}
		go server.BackUpMetaDataByDate(date)
		result.Message = "back job start..."
		w.Write([]byte(server.util.JsonEncodePretty(result)))
	} else {
		result.Message = server.GetClusterNotPermitMessage(r)
		w.Write([]byte(server.util.JsonEncodePretty(result)))
	}
}

func (server *Server) Search(w http.ResponseWriter, r *http.Request) {
	var (
		result    ent.JsonResult
		err       error
		kw        string
		count     int
		fileInfos []ent.FileInfo
		md5s      []string
	)
	kw = r.FormValue("kw")
	if !server.IsPeer(r) {
		result.Message = server.GetClusterNotPermitMessage(r)
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	iter := server.ldb.NewIterator(nil, nil)
	for iter.Next() {
		var fileInfo ent.FileInfo
		value := iter.Value()
		if err = json.Unmarshal(value, &fileInfo); err != nil {
			log.Error(err)
			continue
		}
		if strings.Contains(fileInfo.Name, kw) && !server.util.Contains(fileInfo.Md5, md5s) {
			count = count + 1
			fileInfos = append(fileInfos, fileInfo)
			md5s = append(md5s, fileInfo.Md5)
		}
		if count >= 100 {
			break
		}
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		log.Error()
	}
	//fileInfos=server.SearchDict(kw) // serch file from map for huge capacity
	result.Status = "ok"
	result.Data = fileInfos
	w.Write([]byte(server.util.JsonEncodePretty(result)))
}

func (server *Server) ListDir(w http.ResponseWriter, r *http.Request) {
	var (
		result      ent.JsonResult
		dir         string
		filesInfo   []os.FileInfo
		err         error
		filesResult []ent.FileInfoResult
		tmpDir      string
	)
	if !server.IsPeer(r) {
		result.Message = server.GetClusterNotPermitMessage(r)
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	dir = r.FormValue("dir")
	//if dir == "" {
	//	result.Message = "dir can't null"
	//	w.Write([]byte(server.util.JsonEncodePretty(result)))
	//	return
	//}
	dir = strings.Replace(dir, ".", "", -1)
	if tmpDir, err = os.Readlink(dir); err == nil {
		dir = tmpDir
	}
	filesInfo, err = ioutil.ReadDir(cont.DOCKER_DIR + cont.STORE_DIR_NAME + "/" + dir)
	if err != nil {
		log.Error(err)
		result.Message = err.Error()
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	for _, f := range filesInfo {
		fi := ent.FileInfoResult{
			Name:    f.Name(),
			Size:    f.Size(),
			IsDir:   f.IsDir(),
			ModTime: f.ModTime().Unix(),
			Path:    dir,
			Md5:     server.util.MD5(strings.Replace(cont.STORE_DIR_NAME+"/"+dir+"/"+f.Name(), "//", "/", -1)),
		}
		filesResult = append(filesResult, fi)
	}
	result.Status = "ok"
	result.Data = filesResult
	w.Write([]byte(server.util.JsonEncodePretty(result)))
	return
}

func (server *Server) RemoveEmptyDir(w http.ResponseWriter, r *http.Request) {
	var (
		result ent.JsonResult
	)
	result.Status = "ok"
	if server.IsPeer(r) {
		go server.util.RemoveEmptyDir(cont.DATA_DIR)
		go server.util.RemoveEmptyDir(cont.STORE_DIR)
		result.Message = "clean job start ..,don't try again!!!"
		w.Write([]byte(server.util.JsonEncodePretty(result)))
	} else {
		result.Message = server.GetClusterNotPermitMessage(r)
		w.Write([]byte(server.util.JsonEncodePretty(result)))
	}
}

func (server *Server) RepairFileInfo(w http.ResponseWriter, r *http.Request) {
	var (
		result ent.JsonResult
	)
	if !server.IsPeer(r) {
		w.Write([]byte(server.GetClusterNotPermitMessage(r)))
		return
	}
	if !Config().EnableMigrate {
		w.Write([]byte("please set enable_migrate=true"))
		return
	}
	result.Status = "ok"
	result.Message = "repair job start,don't try again,very danger "
	go server.RepairFileInfoFromFile()
	w.Write([]byte(server.util.JsonEncodePretty(result)))
}

func (server *Server) SyncFileInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err         error
		fileInfo    ent.FileInfo
		fileInfoStr string
		filename    string
	)
	r.ParseForm()
	if !server.IsPeer(r) {
		return
	}
	fileInfoStr = r.FormValue("fileInfo")
	if err = json.Unmarshal([]byte(fileInfoStr), &fileInfo); err != nil {
		w.Write([]byte(server.GetClusterNotPermitMessage(r)))
		log.Error(err)
		return
	}
	if fileInfo.OffSet == -2 {
		// optimize migrate
		server.SaveFileInfoToLevelDB(fileInfo.Md5, &fileInfo, server.ldb)
	} else {
		server.SaveFileMd5Log(&fileInfo, cont.CONST_Md5_QUEUE_FILE_NAME)
	}
	server.AppendToDownloadQueue(&fileInfo)
	filename = fileInfo.Name
	if fileInfo.ReName != "" {
		filename = fileInfo.ReName
	}
	p := strings.Replace(fileInfo.Path, cont.STORE_DIR+"/", "", 1)
	downloadUrl := fmt.Sprintf("http://%s/%s", r.Host, Config().Group+"/"+p+"/"+filename)
	log.Info("SyncFileInfo: ", downloadUrl)
	w.Write([]byte(downloadUrl))
}

func (server *Server) GetMd5sForWeb(w http.ResponseWriter, r *http.Request) {
	var (
		date   string
		err    error
		result mapset.Set
		lines  []string
		md5s   []interface{}
	)
	if !server.IsPeer(r) {
		w.Write([]byte(server.GetClusterNotPermitMessage(r)))
		return
	}
	date = r.FormValue("date")
	if result, err = server.GetMd5sByDate(date, cont.CONST_FILE_Md5_FILE_NAME); err != nil {
		log.Error(err)
		return
	}
	md5s = result.ToSlice()
	for _, line := range md5s {
		if line != nil && line != "" {
			lines = append(lines, line.(string))
		}
	}
	w.Write([]byte(strings.Join(lines, ",")))
}

func (server *Server) ReceiveMd5s(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		md5str   string
		fileInfo *ent.FileInfo
		md5s     []string
	)
	if !server.IsPeer(r) {
		log.Warn(fmt.Sprintf("ReceiveMd5s %s", server.util.GetClientIp(r)))
		w.Write([]byte(server.GetClusterNotPermitMessage(r)))
		return
	}
	r.ParseForm()
	md5str = r.FormValue("md5s")
	md5s = strings.Split(md5str, ",")
	AppendFunc := func(md5s []string) {
		for _, m := range md5s {
			if m != "" {
				if fileInfo, err = server.GetFileInfoFromLevelDB(m); err != nil {
					log.Error(err)
					continue
				}
				server.AppendToQueue(fileInfo)
			}
		}
	}
	go AppendFunc(md5s)
}

func (server *Server) GenGoogleSecret(w http.ResponseWriter, r *http.Request) {
	var (
		result ent.JsonResult
	)
	result.Status = "ok"
	result.Message = "ok"
	if !server.IsPeer(r) {
		result.Message = server.GetClusterNotPermitMessage(r)
		w.Write([]byte(server.util.JsonEncodePretty(result)))
	}
	GetSeed := func(length int) string {
		seeds := "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
		s := ""
		random.Seed(time.Now().UnixNano())
		for i := 0; i < length; i++ {
			s += string(seeds[random.Intn(32)])
		}
		return s
	}
	result.Data = GetSeed(16)
	w.Write([]byte(server.util.JsonEncodePretty(result)))
}

func (server *Server) GenGoogleCode(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		result ent.JsonResult
		secret string
		goauth *googleAuthenticator.GAuth
	)
	r.ParseForm()
	goauth = googleAuthenticator.NewGAuth()
	secret = r.FormValue("secret")
	result.Status = "ok"
	result.Message = "ok"
	if !server.IsPeer(r) {
		result.Message = server.GetClusterNotPermitMessage(r)
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	if result.Data, err = goauth.GetCode(secret); err != nil {
		result.Message = err.Error()
		w.Write([]byte(server.util.JsonEncodePretty(result)))
		return
	}
	w.Write([]byte(server.util.JsonEncodePretty(result)))
}

func (server *Server) Download(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		ok        bool
		fullpath  string
		smallPath string
		fi        os.FileInfo
	)
	// redirect to upload
	if r.RequestURI == "/" || r.RequestURI == "" ||
		r.RequestURI == "/"+Config().Group ||
		r.RequestURI == "/"+Config().Group+"/" {
		server.Index(w, r)
		return
	}
	if ok, err = server.CheckDownloadAuth(w, r); !ok {
		log.Error(err)
		server.NotPermit(w, r)
		return
	}

	if Config().EnableCrossOrigin {
		server.CrossOrigin(w, r)
	}
	fullpath, smallPath = server.GetFilePathFromRequest(w, r)
	if smallPath == "" {
		if fi, err = os.Stat(fullpath); err != nil {
			server.DownloadNotFound(w, r)
			return
		}
		if !Config().ShowDir && fi.IsDir() {
			w.Write([]byte("list dir deny"))
			return
		}
		//staticHandler.ServeHTTP(w, r)
		server.DownloadNormalFileByURI(w, r)
		return
	}
	if smallPath != "" {
		if ok, err = server.DownloadSmallFileByURI(w, r); !ok {
			server.DownloadNotFound(w, r)
			return
		}
		return
	}

}
