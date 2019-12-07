package serv

import (
	"bufio"
	"fmt"
	"github.com/astaxie/beego/httplib"
	mapset "github.com/deckarep/golang-set"
	_ "github.com/eventials/go-tus"
	"github.com/radovskyb/watcher"
	log "github.com/sjqzhang/seelog"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/thinxz-yuan/go-fastdfs/serv/cont"
	"github.com/thinxz-yuan/go-fastdfs/serv/ent"
	"io/ioutil"
	_ "net/http/pprof"
	"net/smtp"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

//
func (server *Server) checkFileAndSendToPeer(date string, filename string, isForceUpload bool) {
	var (
		md5set mapset.Set
		err    error
		md5s   []interface{}
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("CheckFileAndSendToPeer")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	if md5set, err = server.GetMd5sByDate(date, filename); err != nil {
		log.Error(err)
		return
	}
	md5s = md5set.ToSlice()
	for _, md := range md5s {
		if md == nil {
			continue
		}
		if fileInfo, _ := server.GetFileInfoFromLevelDB(md.(string)); fileInfo != nil && fileInfo.Md5 != "" {
			if isForceUpload {
				fileInfo.Peers = []string{}
			}
			if len(fileInfo.Peers) > len(Config().Peers) {
				continue
			}
			if !server.util.Contains(server.host, fileInfo.Peers) {
				fileInfo.Peers = append(fileInfo.Peers, server.host) // peer is null
			}
			if filename == cont.CONST_Md5_QUEUE_FILE_NAME {
				server.AppendToDownloadQueue(fileInfo)
			} else {
				server.AppendToQueue(fileInfo)
			}
		}
	}
}

//
func (server *Server) cleanAndBackUp() {
	Clean := func() {
		var (
			filenames []string
			yesterday string
		)
		if server.curDate != server.util.GetToDay() {
			filenames = []string{cont.CONST_Md5_QUEUE_FILE_NAME, cont.CONST_Md5_ERROR_FILE_NAME, cont.CONST_REMOME_Md5_FILE_NAME}
			yesterday = server.util.GetDayFromTimeStamp(time.Now().AddDate(0, 0, -1).Unix())
			for _, filename := range filenames {
				server.cleanLogLevelDBByDate(yesterday, filename)
			}
			server.BackUpMetaDataByDate(yesterday)
			server.curDate = server.util.GetToDay()
		}
	}
	go func() {
		for {
			time.Sleep(time.Hour * 6)
			Clean()
		}
	}()
}
func (server *Server) cleanLogLevelDBByDate(date string, filename string) {
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("CleanLogLevelDBByDate")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	var (
		err       error
		keyPrefix string
		keys      mapset.Set
	)
	keys = mapset.NewSet()
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, filename)
	iter := server.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	for iter.Next() {
		keys.Add(string(iter.Value()))
	}
	iter.Release()
	for key := range keys.Iter() {
		err = server.RemoveKeyFromLevelDB(key.(string), server.logDB)
		if err != nil {
			log.Error(err)
		}
	}
}

//
func (server *Server) checkClusterStatus() {
	check := func() {
		defer func() {
			if re := recover(); re != nil {
				buffer := debug.Stack()
				log.Error("CheckClusterStatus")
				log.Error(re)
				log.Error(string(buffer))
			}
		}()
		var (
			status  ent.JsonResult
			err     error
			subject string
			body    string
			req     *httplib.BeegoHTTPRequest
		)
		for _, peer := range Config().Peers {
			req = httplib.Get(fmt.Sprintf("%s%s", peer, server.getRequestURI("status")))
			req.SetTimeout(time.Second*5, time.Second*5)
			err = req.ToJSON(&status)
			if err != nil || status.Status != "ok" {
				for _, to := range Config().AlarmReceivers {
					subject = "fastdfs server error"
					if err != nil {
						body = fmt.Sprintf("%s\nserver:%s\nerror:\n%s", subject, peer, err.Error())
					} else {
						body = fmt.Sprintf("%s\nserver:%s\n", subject, peer)
					}
					if err = server.sendToMail(to, subject, body, "text"); err != nil {
						log.Error(err)
					}
				}
				if Config().AlarmUrl != "" {
					req = httplib.Post(Config().AlarmUrl)
					req.SetTimeout(time.Second*10, time.Second*10)
					req.Param("message", body)
					req.Param("subject", subject)
					if _, err = req.String(); err != nil {
						log.Error(err)
					}
				}
			}
		}
	}
	go func() {
		for {
			time.Sleep(time.Minute * 10)
			check()
		}
	}()
}
func (server *Server) sendToMail(to, subject, body, mailtype string) error {
	host := Config().Mail.Host
	user := Config().Mail.User
	password := Config().Mail.Password
	hp := strings.Split(host, ":")
	auth := smtp.PlainAuth("", user, password, hp[0])
	var contentType string
	if mailtype == "html" {
		contentType = "Content-Type: text/" + mailtype + "; charset=UTF-8"
	} else {
		contentType = "Content-Type: text/plain" + "; charset=UTF-8"
	}
	msg := []byte("To: " + to + "\r\nFrom: " + user + ">\r\nSubject: " + "\r\n" + contentType + "\r\n\r\n" + body)
	sendTo := strings.Split(to, ";")
	err := smtp.SendMail(host, auth, user, sendTo, msg)
	return err
}

func (server *Server) loadQueueSendToPeer() {
	if queue, err := server.loadFileInfoByDate(server.util.GetToDay(), cont.CONST_Md5_QUEUE_FILE_NAME); err != nil {
		log.Error(err)
	} else {
		for fileInfo := range queue.Iter() {
			//server.queueFromPeers <- *fileInfo.(*FileInfo)
			server.AppendToDownloadQueue(fileInfo.(*ent.FileInfo))
		}
	}
}
func (server *Server) loadFileInfoByDate(date string, filename string) (mapset.Set, error) {
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("LoadFileInfoByDate")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	var (
		err       error
		keyPrefix string
		fileInfos mapset.Set
	)
	fileInfos = mapset.NewSet()
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, filename)
	iter := server.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	for iter.Next() {
		var fileInfo ent.FileInfo
		if err = json.Unmarshal(iter.Value(), &fileInfo); err != nil {
			continue
		}
		fileInfos.Add(&fileInfo)
	}
	iter.Release()
	return fileInfos, nil
}

//
func (server *Server) consumerPostToPeer() {
	ConsumerFunc := func() {
		for {
			fileInfo := <-server.queueToPeers
			server.postFileToPeer(&fileInfo)
		}
	}
	for i := 0; i < Config().SyncWorker; i++ {
		go ConsumerFunc()
	}
}

//
func (server *Server) consumerLog() {
	go func() {
		var (
			fileLog *ent.FileLog
		)
		for {
			fileLog = <-server.queueFileLog
			server.saveFileMd5Log(fileLog.FileInfo, fileLog.FileName)
		}
	}()
}

//
func (server *Server) consumerDownLoad() {
	ConsumerFunc := func() {
		for {
			fileInfo := <-server.queueFromPeers
			if len(fileInfo.Peers) <= 0 {
				log.Warn("Peer is null", fileInfo)
				continue
			}
			for _, peer := range fileInfo.Peers {
				if strings.Contains(peer, "127.0.0.1") {
					log.Warn("sync error with 127.0.0.1", fileInfo)
					continue
				}
				if peer != server.host {
					server.DownloadFromPeer(peer, &fileInfo)
					break
				}
			}
		}
	}
	for i := 0; i < Config().SyncWorker; i++ {
		go ConsumerFunc()
	}
}

//
func (server *Server) consumerUpload() {
	ConsumerFunc := func() {
		for {
			wr := <-server.queueUpload
			server.upload(*wr.W, wr.R)
			server.rtMap.AddCountInt64(cont.CONST_UPLOAD_COUNTER_KEY, wr.R.ContentLength)
			if v, ok := server.rtMap.GetValue(cont.CONST_UPLOAD_COUNTER_KEY); ok {
				if v.(int64) > 1*1024*1024*1024 {
					var _v int64
					server.rtMap.Put(cont.CONST_UPLOAD_COUNTER_KEY, _v)
					debug.FreeOSMemory()
				}
			}
			wr.Done <- true
		}
	}
	for i := 0; i < Config().UploadWorker; i++ {
		go ConsumerFunc()
	}
}

//
func (server *Server) removeDownloading() {
	RemoveDownloadFunc := func() {
		for {
			iter := server.ldb.NewIterator(util.BytesPrefix([]byte("downloading_")), nil)
			for iter.Next() {
				key := iter.Key()
				keys := strings.Split(string(key), "_")
				if len(keys) == 3 {
					if t, err := strconv.ParseInt(keys[1], 10, 64); err == nil && time.Now().Unix()-t > 60*10 {
						os.Remove(cont.DOCKER_DIR + keys[2])
					}
				}
			}
			iter.Release()
			time.Sleep(time.Minute * 3)
		}
	}
	go RemoveDownloadFunc()
}

//
func (server *Server) watchFilesChange() {
	var (
		w        *watcher.Watcher
		fileInfo ent.FileInfo
		curDir   string
		err      error
		qchan    chan *ent.FileInfo
		isLink   bool
	)
	qchan = make(chan *ent.FileInfo, 10000)
	w = watcher.New()
	w.FilterOps(watcher.Create)
	//w.FilterOps(watcher.Create, watcher.Remove)
	curDir, err = filepath.Abs(filepath.Dir(cont.STORE_DIR_NAME))
	if err != nil {
		log.Error(err)
	}
	go func() {
		for {
			select {
			case event := <-w.Event:
				if event.IsDir() {
					continue
				}

				fpath := strings.Replace(event.Path, curDir+string(os.PathSeparator), "", 1)
				if isLink {
					fpath = strings.Replace(event.Path, curDir, cont.STORE_DIR_NAME, 1)
				}
				fpath = strings.Replace(fpath, string(os.PathSeparator), "/", -1)
				sum := server.util.MD5(fpath)
				fileInfo = ent.FileInfo{
					Size:      event.Size(),
					Name:      event.Name(),
					Path:      strings.TrimSuffix(fpath, "/"+event.Name()), // files/default/20190927/xxx
					Md5:       sum,
					TimeStamp: event.ModTime().Unix(),
					Peers:     []string{server.host},
					OffSet:    -2,
					Op:        event.Op.String(),
				}
				log.Info(fmt.Sprintf("WatchFilesChange op:%s path:%s", event.Op.String(), fpath))
				qchan <- &fileInfo
				//server.AppendToQueue(&fileInfo)
			case err := <-w.Error:
				log.Error(err)
			case <-w.Closed:
				return
			}
		}
	}()
	go func() {
		for {
			c := <-qchan
			if time.Now().Unix()-c.TimeStamp < 3 {
				qchan <- c
				time.Sleep(time.Second * 1)
				continue
			} else {
				//if c.op == watcher.Remove.String() {
				//	req := httplib.Post(fmt.Sprintf("%s%s?md5=%s", server.host, server.getRequestURI("delete"), c.Md5))
				//	req.Param("md5", c.Md5)
				//	req.SetTimeout(time.Second*5, time.Second*10)
				//	log.Infof(req.String())
				//}
				if c.Op == watcher.Create.String() {
					log.Info(fmt.Sprintf("Syncfile Add to Queue path:%s", fileInfo.Path+"/"+fileInfo.Name))
					server.AppendToQueue(c)
					server.SaveFileInfoToLevelDB(c.Md5, c, server.ldb)
				}
			}
		}
	}()
	if dir, err := os.Readlink(cont.STORE_DIR_NAME); err == nil {

		if strings.HasSuffix(dir, string(os.PathSeparator)) {
			dir = strings.TrimSuffix(dir, string(os.PathSeparator))
		}
		curDir = dir
		isLink = true
		if err := w.AddRecursive(dir); err != nil {
			log.Error(err)
		}
		w.Ignore(dir + "/_tmp/")
		w.Ignore(dir + "/" + cont.LARGE_DIR_NAME + "/")
	}
	if err := w.AddRecursive("./" + cont.STORE_DIR_NAME); err != nil {
		log.Error(err)
	}
	w.Ignore("./" + cont.STORE_DIR_NAME + "/_tmp/")
	w.Ignore("./" + cont.STORE_DIR_NAME + "/" + cont.LARGE_DIR_NAME + "/")
	if err := w.Start(time.Millisecond * 100); err != nil {
		log.Error(err)
	}
}

//
func (server *Server) loadSearchDict() {
	go func() {
		log.Info("Load search dict ....")
		f, err := os.Open(cont.CONST_SEARCH_FILE_NAME)
		if err != nil {
			log.Error(err)
			return
		}
		defer f.Close()
		r := bufio.NewReader(f)
		for {
			line, isprefix, err := r.ReadLine()
			for isprefix && err == nil {
				kvs := strings.Split(string(line), "\t")
				if len(kvs) == 2 {
					server.searchMap.Put(kvs[0], kvs[1])
				}
			}
		}
		log.Info("finish load search dict")
	}()
}

//
func (server *Server) repairFileInfoFromFile() {
	var (
		pathPrefix string
		err        error
		fi         os.FileInfo
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("RepairFileInfoFromFile")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	if server.lockMap.IsLock("RepairFileInfoFromFile") {
		log.Warn("Lock RepairFileInfoFromFile")
		return
	}
	server.lockMap.LockKey("RepairFileInfoFromFile")
	defer server.lockMap.UnLockKey("RepairFileInfoFromFile")
	handlefunc := func(file_path string, f os.FileInfo, err error) error {
		var (
			files    []os.FileInfo
			fi       os.FileInfo
			fileInfo ent.FileInfo
			sum      string
			pathMd5  string
		)
		if f.IsDir() {
			files, err = ioutil.ReadDir(file_path)

			if err != nil {
				return err
			}
			for _, fi = range files {
				if fi.IsDir() || fi.Size() == 0 {
					continue
				}
				file_path = strings.Replace(file_path, "\\", "/", -1)
				if cont.DOCKER_DIR != "" {
					file_path = strings.Replace(file_path, cont.DOCKER_DIR, "", 1)
				}
				if pathPrefix != "" {
					file_path = strings.Replace(file_path, pathPrefix, cont.STORE_DIR_NAME, 1)
				}
				if strings.HasPrefix(file_path, cont.STORE_DIR_NAME+"/"+cont.LARGE_DIR_NAME) {
					log.Info(fmt.Sprintf("ignore small file file %s", file_path+"/"+fi.Name()))
					continue
				}
				pathMd5 = server.util.MD5(file_path + "/" + fi.Name())
				//if finfo, _ := server.GetFileInfoFromLevelDB(pathMd5); finfo != nil && finfo.Md5 != "" {
				//	log.Info(fmt.Sprintf("exist ignore file %s", file_path+"/"+fi.Name()))
				//	continue
				//}
				//sum, err = server.util.GetFileSumByName(file_path+"/"+fi.Name(), Config().FileSumArithmetic)
				sum = pathMd5
				if err != nil {
					log.Error(err)
					continue
				}
				fileInfo = ent.FileInfo{
					Size:      fi.Size(),
					Name:      fi.Name(),
					Path:      file_path,
					Md5:       sum,
					TimeStamp: fi.ModTime().Unix(),
					Peers:     []string{server.host},
					OffSet:    -2,
				}
				//log.Info(fileInfo)
				log.Info(file_path, "/", fi.Name())
				server.AppendToQueue(&fileInfo)
				//server.postFileToPeer(&fileInfo)
				server.SaveFileInfoToLevelDB(fileInfo.Md5, &fileInfo, server.ldb)
				//server.SaveFileMd5Log(&fileInfo, CONST_FILE_Md5_FILE_NAME)
			}
		}
		return nil
	}
	pathname := cont.STORE_DIR
	pathPrefix, err = os.Readlink(pathname)
	if err == nil {
		//link
		pathname = pathPrefix
		if strings.HasSuffix(pathPrefix, "/") {
			//bugfix fullpath
			pathPrefix = pathPrefix[0 : len(pathPrefix)-1]
		}
	}
	fi, err = os.Stat(pathname)
	if err != nil {
		log.Error(err)
	}
	if fi.IsDir() {
		filepath.Walk(pathname, handlefunc)
	}
	log.Info("RepairFileInfoFromFile is finish.")
}

//
func (server *Server) autoRepair(forceRepair bool) {
	if server.lockMap.IsLock("AutoRepair") {
		log.Warn("Lock AutoRepair")
		return
	}
	server.lockMap.LockKey("AutoRepair")
	defer server.lockMap.UnLockKey("AutoRepair")
	AutoRepairFunc := func(forceRepair bool) {
		var (
			dateStats []ent.StatDateFileInfo
			err       error
			countKey  string
			md5s      string
			localSet  mapset.Set
			remoteSet mapset.Set
			allSet    mapset.Set
			tmpSet    mapset.Set
			fileInfo  *ent.FileInfo
		)
		defer func() {
			if re := recover(); re != nil {
				buffer := debug.Stack()
				log.Error("AutoRepair")
				log.Error(re)
				log.Error(string(buffer))
			}
		}()
		Update := func(peer string, dateStat ent.StatDateFileInfo) {
			//从远端拉数据过来
			req := httplib.Get(fmt.Sprintf("%s%s?date=%s&force=%s", peer, server.getRequestURI("sync"), dateStat.Date, "1"))
			req.SetTimeout(time.Second*5, time.Second*5)
			if _, err = req.String(); err != nil {
				log.Error(err)
			}
			log.Info(fmt.Sprintf("syn file from %s date %s", peer, dateStat.Date))
		}
		for _, peer := range Config().Peers {
			req := httplib.Post(fmt.Sprintf("%s%s", peer, server.getRequestURI("stat")))
			req.Param("inner", "1")
			req.SetTimeout(time.Second*5, time.Second*15)
			if err = req.ToJSON(&dateStats); err != nil {
				log.Error(err)
				continue
			}
			for _, dateStat := range dateStats {
				if dateStat.Date == "all" {
					continue
				}
				countKey = dateStat.Date + "_" + cont.CONST_STAT_FILE_COUNT_KEY
				if v, ok := server.statMap.GetValue(countKey); ok {
					switch v.(type) {
					case int64:
						if v.(int64) != dateStat.FileCount || forceRepair {
							//不相等,找差异
							//TODO
							req := httplib.Post(fmt.Sprintf("%s%s", peer, server.getRequestURI("get_md5s_by_date")))
							req.SetTimeout(time.Second*15, time.Second*60)
							req.Param("date", dateStat.Date)
							if md5s, err = req.String(); err != nil {
								continue
							}
							if localSet, err = server.GetMd5sByDate(dateStat.Date, cont.CONST_FILE_Md5_FILE_NAME); err != nil {
								log.Error(err)
								continue
							}
							remoteSet = server.util.StrToMapSet(md5s, ",")
							allSet = localSet.Union(remoteSet)
							md5s = server.util.MapSetToStr(allSet.Difference(localSet), ",")
							req = httplib.Post(fmt.Sprintf("%s%s", peer, server.getRequestURI("receive_md5s")))
							req.SetTimeout(time.Second*15, time.Second*60)
							req.Param("md5s", md5s)
							req.String()
							tmpSet = allSet.Difference(remoteSet)
							for v := range tmpSet.Iter() {
								if v != nil {
									if fileInfo, err = server.GetFileInfoFromLevelDB(v.(string)); err != nil {
										log.Error(err)
										continue
									}
									server.AppendToQueue(fileInfo)
								}
							}
							//Update(peer,dateStat)
						}
					}
				} else {
					Update(peer, dateStat)
				}
			}
		}
	}
	AutoRepairFunc(forceRepair)
}