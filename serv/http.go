package serv

import (
	"fmt"
	_ "github.com/eventials/go-tus"
	log "github.com/sjqzhang/seelog"
	"github.com/thinxz-yuan/go-fastdfs/common"
	"net/http"
	_ "net/http/pprof"
	"runtime/debug"
	"time"
)

type HttpServer struct {
	server *Server
}

func NewHttpServer(server *Server, groupRoute string) *HttpServer {
	hs := &HttpServer{
		server,
	}
	// 初始化
	hs.initHttpServer(groupRoute)
	return hs
}

type HttpHandler struct {
	hs *HttpServer
}

func (hh *HttpHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	status_code := "200"
	defer func(t time.Time) {
		logStr := fmt.Sprintf("[Access] %s | %s | %s | %s | %s |%s",
			time.Now().Format("2006/01/02 - 15:04:05"),
			//res.Header(),
			time.Since(t).String(),
			common.Util.GetClientIp(req),
			req.Method,
			status_code,
			req.RequestURI,
		)
		hh.hs.server.GetLogger().Info(logStr)
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
		common.CrossOrigin(res, req)
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
		http.HandleFunc(fmt.Sprintf("%s", "/"), hs.Download)
		http.HandleFunc(fmt.Sprintf("/%s", uploadPage), hs.Index)
	} else {
		http.HandleFunc(fmt.Sprintf("%s", "/"), hs.Download)
		http.HandleFunc(fmt.Sprintf("%s", groupRoute), hs.Download)
		http.HandleFunc(fmt.Sprintf("%s/%s", groupRoute, uploadPage), hs.Index)
	}

	http.HandleFunc(fmt.Sprintf("%s/check_files_exist", groupRoute), hs.CheckFilesExist)
	http.HandleFunc(fmt.Sprintf("%s/check_file_exist", groupRoute), hs.CheckFileExist)
	http.HandleFunc(fmt.Sprintf("%s/upload", groupRoute), hs.Upload)
	http.HandleFunc(fmt.Sprintf("%s/delete", groupRoute), hs.RemoveFile)
	http.HandleFunc(fmt.Sprintf("%s/get_file_info", groupRoute), hs.GetFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/sync", groupRoute), hs.Sync)
	http.HandleFunc(fmt.Sprintf("%s/stat", groupRoute), hs.Stat)
	http.HandleFunc(fmt.Sprintf("%s/repair_stat", groupRoute), hs.RepairStatWeb)
	http.HandleFunc(fmt.Sprintf("%s/status", groupRoute), hs.Status)
	http.HandleFunc(fmt.Sprintf("%s/repair", groupRoute), hs.Repair)
	http.HandleFunc(fmt.Sprintf("%s/report", groupRoute), hs.Report)
	http.HandleFunc(fmt.Sprintf("%s/backup", groupRoute), hs.BackUp)
	http.HandleFunc(fmt.Sprintf("%s/search", groupRoute), hs.Search)
	http.HandleFunc(fmt.Sprintf("%s/list_dir", groupRoute), hs.ListDir)
	http.HandleFunc(fmt.Sprintf("%s/remove_empty_dir", groupRoute), hs.RemoveEmptyDir)
	http.HandleFunc(fmt.Sprintf("%s/repair_fileinfo", groupRoute), hs.RepairFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/reload", groupRoute), hs.server.Reload)
	http.HandleFunc(fmt.Sprintf("%s/syncfile_info", groupRoute), hs.SyncFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/get_md5s_by_date", groupRoute), hs.GetMd5sForWeb)
	http.HandleFunc(fmt.Sprintf("%s/receive_md5s", groupRoute), hs.ReceiveMd5s)
	http.HandleFunc(fmt.Sprintf("%s/gen_google_secret", groupRoute), hs.GenGoogleSecret)
	http.HandleFunc(fmt.Sprintf("%s/gen_google_code", groupRoute), hs.GenGoogleCode)
	http.HandleFunc("/"+common.Config().Group+"/", hs.Download)

	//
	fmt.Println("Listen on " + common.Config().Addr)
	srv := &http.Server{
		Addr:              common.Config().Addr,
		Handler:           &HttpHandler{hs},
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
