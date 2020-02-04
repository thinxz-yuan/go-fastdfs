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

type HttpServer struct {
	server *serv.Server
}

func NewHttpServer(server *serv.Server, groupRoute string) *HttpServer {
	hs := &HttpServer{
		server,
	}
	// 初始化
	hs.initHttpServer(groupRoute)
	return hs
}

type HttpHandler struct {
	server *serv.Server
}

func (hh *HttpHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	status_code := "200"
	defer func(t time.Time) {
		logStr := fmt.Sprintf("[Access] %s | %s | %s | %s | %s |%s",
			time.Now().Format("2006/01/02 - 15:04:05"),
			//res.Header(),
			time.Since(t).String(),
			hh.server.GetUtil().GetClientIp(req),
			req.Method,
			status_code,
			req.RequestURI,
		)
		hh.server.GetLogger().Info(logStr)
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
		hh.server.CrossOrigin(res, req)
	}
	http.DefaultServeMux.ServeHTTP(res, req)
}

// 开启Web服务, 提供对外接口
// ----------------------------------------
// server
// groupRoute 分组(分组路由, Server 对应分组)
// ----------------------------------------
func (hs *HttpServer) initHttpServer(groupRoute string) {
	//
	uploadPage := "upload.html"
	if groupRoute == "" {
		http.HandleFunc(fmt.Sprintf("%s", "/"), hs.server.Download)
		http.HandleFunc(fmt.Sprintf("/%s", uploadPage), hs.server.Index)
	} else {
		http.HandleFunc(fmt.Sprintf("%s", "/"), hs.server.Download)
		http.HandleFunc(fmt.Sprintf("%s", groupRoute), hs.server.Download)
		http.HandleFunc(fmt.Sprintf("%s/%s", groupRoute, uploadPage), hs.server.Index)
	}

	http.HandleFunc(fmt.Sprintf("%s/check_files_exist", groupRoute), hs.server.CheckFilesExist)
	http.HandleFunc(fmt.Sprintf("%s/check_file_exist", groupRoute), hs.server.CheckFileExist)
	http.HandleFunc(fmt.Sprintf("%s/upload", groupRoute), hs.server.Upload)
	http.HandleFunc(fmt.Sprintf("%s/delete", groupRoute), hs.server.RemoveFile)
	http.HandleFunc(fmt.Sprintf("%s/get_file_info", groupRoute), hs.server.GetFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/sync", groupRoute), hs.server.Sync)
	http.HandleFunc(fmt.Sprintf("%s/stat", groupRoute), hs.server.Stat)
	http.HandleFunc(fmt.Sprintf("%s/repair_stat", groupRoute), hs.server.RepairStatWeb)
	http.HandleFunc(fmt.Sprintf("%s/status", groupRoute), hs.server.Status)
	http.HandleFunc(fmt.Sprintf("%s/repair", groupRoute), hs.server.Repair)
	http.HandleFunc(fmt.Sprintf("%s/report", groupRoute), hs.server.Report)
	http.HandleFunc(fmt.Sprintf("%s/backup", groupRoute), hs.server.BackUp)
	http.HandleFunc(fmt.Sprintf("%s/search", groupRoute), hs.server.Search)
	http.HandleFunc(fmt.Sprintf("%s/list_dir", groupRoute), hs.server.ListDir)
	http.HandleFunc(fmt.Sprintf("%s/remove_empty_dir", groupRoute), hs.server.RemoveEmptyDir)
	http.HandleFunc(fmt.Sprintf("%s/repair_fileinfo", groupRoute), hs.server.RepairFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/reload", groupRoute), hs.server.Reload)
	http.HandleFunc(fmt.Sprintf("%s/syncfile_info", groupRoute), hs.server.SyncFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/get_md5s_by_date", groupRoute), hs.server.GetMd5sForWeb)
	http.HandleFunc(fmt.Sprintf("%s/receive_md5s", groupRoute), hs.server.ReceiveMd5s)
	http.HandleFunc(fmt.Sprintf("%s/gen_google_secret", groupRoute), hs.server.GenGoogleSecret)
	http.HandleFunc(fmt.Sprintf("%s/gen_google_code", groupRoute), hs.server.GenGoogleCode)
	http.HandleFunc("/"+common.Config().Group+"/", hs.server.Download)

	//
	fmt.Println("Listen on " + common.Config().Addr)
	srv := &http.Server{
		Addr:              common.Config().Addr,
		Handler:           &HttpHandler{hs.server},
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
