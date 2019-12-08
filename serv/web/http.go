package web

import (
	"fmt"
	_ "github.com/eventials/go-tus"
	log "github.com/sjqzhang/seelog"
	"github.com/thinxz-yuan/go-fastdfs/serv"
	"github.com/thinxz-yuan/go-fastdfs/serv/config"
	"net/http"
	_ "net/http/pprof"
	"runtime/debug"
	"time"
)

var s *serv.Server

type HttpHandler struct {
}

func (HttpHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	status_code := "200"
	defer func(t time.Time) {
		logStr := fmt.Sprintf("[Access] %s | %s | %s | %s | %s |%s",
			time.Now().Format("2006/01/02 - 15:04:05"),
			//res.Header(),
			time.Since(t).String(),
			s.GetUtil().GetClientIp(req),
			req.Method,
			status_code,
			req.RequestURI,
		)
		s.GetLogger().Info(logStr)
	}(time.Now())
	defer func() {
		if err := recover(); err != nil {
			status_code = "500"
			res.WriteHeader(500)
			print(err)
			buff := debug.Stack()
			log.Error(err)
			log.Error(string(buff))
		}
	}()
	if config.Config().EnableCrossOrigin {
		s.CrossOrigin(res, req)
	}
	http.DefaultServeMux.ServeHTTP(res, req)
}

//
//type HookDataStore struct {
//	tusd.DataStore
//}
//
//func (store HookDataStore) NewUpload(info tusd.FileInfo) (id string, err error) {
//	var (
//		jsonResult ent.JsonResult
//	)
//	if config.Config().AuthUrl != "" {
//		if auth_token, ok := info.MetaData["auth_token"]; !ok {
//			msg := "token auth fail,auth_token is not in http header Upload-Metadata," +
//				"in uppy uppy.setMeta({ auth_token: '9ee60e59-cb0f-4578-aaba-29b9fc2919ca' })"
//			log.Error(msg, fmt.Sprintf("current header:%v", info.MetaData))
//			return "", httpError{error: errors.New(msg), statusCode: 401}
//		} else {
//			req := httplib.Post(config.Config().AuthUrl)
//			req.Param("auth_token", auth_token)
//			req.SetTimeout(time.Second*5, time.Second*10)
//			content, err := req.String()
//			content = strings.TrimSpace(content)
//			if strings.HasPrefix(content, "{") && strings.HasSuffix(content, "}") {
//				if err = json.Unmarshal([]byte(content), &jsonResult); err != nil {
//					log.Error(err)
//					return "", HttpError{error: errors.New(err.Error() + content), statusCode: 401}
//				}
//				if jsonResult.Data != "ok" {
//					return "", HttpError{error: errors.New(content), statusCode: 401}
//				}
//			} else {
//				if err != nil {
//					log.Error(err)
//					return "", err
//				}
//				if strings.TrimSpace(content) != "ok" {
//					return "", httpError{error: errors.New(content), statusCode: 401}
//				}
//			}
//		}
//	}
//	return store.DataStore.NewUpload(info)
//}
//
//type HttpError struct {
//	error
//	statusCode int
//}
//
//func (err HttpError) StatusCode() int {
//	return err.statusCode
//}
//
//func (err HttpError) Body() []byte {
//	return []byte(err.Error())
//}

// 开启Web服务, 提供对外接口
// ----------------------------------------
// server
// groupRoute 分组(分组路由, Server 对应分组)
// ----------------------------------------
func StartHttpServe(server *serv.Server, groupRoute string) {
	//
	s = server

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
	http.HandleFunc(fmt.Sprintf("%s/reload", groupRoute), server.Reload)
	http.HandleFunc(fmt.Sprintf("%s/syncfile_info", groupRoute), server.SyncFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/get_md5s_by_date", groupRoute), server.GetMd5sForWeb)
	http.HandleFunc(fmt.Sprintf("%s/receive_md5s", groupRoute), server.ReceiveMd5s)
	http.HandleFunc(fmt.Sprintf("%s/gen_google_secret", groupRoute), server.GenGoogleSecret)
	http.HandleFunc(fmt.Sprintf("%s/gen_google_code", groupRoute), server.GenGoogleCode)
	http.HandleFunc("/"+config.Config().Group+"/", server.Download)

	//
	fmt.Println("Listen on " + config.Config().Addr)
	srv := &http.Server{
		Addr:              config.Config().Addr,
		Handler:           new(HttpHandler),
		ReadTimeout:       time.Duration(config.Config().ReadTimeout) * time.Second,
		ReadHeaderTimeout: time.Duration(config.Config().ReadHeaderTimeout) * time.Second,
		WriteTimeout:      time.Duration(config.Config().WriteTimeout) * time.Second,
		IdleTimeout:       time.Duration(config.Config().IdleTimeout) * time.Second,
	}

	// 开启HTTP服务, (阻塞主线程)
	err := srv.ListenAndServe()

	//
	_ = log.Error(err)
	fmt.Println(err)
}
