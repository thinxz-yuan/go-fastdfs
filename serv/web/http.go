package web

import (
	"fmt"
	_ "github.com/eventials/go-tus"
	log "github.com/sjqzhang/seelog"
	"github.com/thinxz-yuan/go-fastdfs/common"
	"github.com/thinxz-yuan/go-fastdfs/serv"
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
	if common.Config().EnableCrossOrigin {
		s.CrossOrigin(res, req)
	}
	http.DefaultServeMux.ServeHTTP(res, req)
}

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
	http.HandleFunc("/"+common.Config().Group+"/", server.Download)

	//
	fmt.Println("Listen on " + common.Config().Addr)
	srv := &http.Server{
		Addr:              common.Config().Addr,
		Handler:           new(HttpHandler),
		ReadTimeout:       time.Duration(common.Config().ReadTimeout) * time.Second,
		ReadHeaderTimeout: time.Duration(common.Config().ReadHeaderTimeout) * time.Second,
		WriteTimeout:      time.Duration(common.Config().WriteTimeout) * time.Second,
		IdleTimeout:       time.Duration(common.Config().IdleTimeout) * time.Second,
	}

	// 开启HTTP服务, (阻塞主线程)
	err := srv.ListenAndServe()

	//
	_ = log.Error(err)
	fmt.Println(err)
}
