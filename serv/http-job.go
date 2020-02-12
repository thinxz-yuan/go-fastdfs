package serv

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/astaxie/beego/httplib"
	mapset "github.com/deckarep/golang-set"
	_ "github.com/eventials/go-tus"
	"github.com/nfnt/resize"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	"github.com/sjqzhang/googleAuthenticator"
	log "github.com/sjqzhang/seelog"
	"github.com/thinxz-yuan/go-fastdfs/common"
	"github.com/thinxz-yuan/go-fastdfs/serv/cont"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"io/ioutil"
	random "math/rand"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

//
func (hs *HttpServer) Download(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		ok        bool
		fullpath  string
		smallPath string
		fi        os.FileInfo
	)
	// redirect to upload
	if r.RequestURI == "/" || r.RequestURI == "" ||
		r.RequestURI == "/"+common.Config().Group ||
		r.RequestURI == "/"+common.Config().Group+"/" {
		hs.Index(w, r)
		return
	}
	if ok, err = hs.checkDownloadAuth(w, r); !ok {
		log.Error(err)
		hs.server.NotPermit(w, r)
		return
	}

	if common.Config().EnableCrossOrigin {
		common.CrossOrigin(w, r)
	}
	fullpath, smallPath = hs.getFilePathFromRequest(w, r)
	if smallPath == "" {
		if fi, err = os.Stat(fullpath); err != nil {
			hs.downloadNotFound(w, r)
			return
		}
		if !common.Config().ShowDir && fi.IsDir() {
			w.Write([]byte("list dir deny"))
			return
		}
		//staticHandler.ServeHTTP(w, r)
		hs.downloadNormalFileByURI(w, r)
		return
	}
	if smallPath != "" {
		if ok, err = hs.downloadSmallFileByURI(w, r); !ok {
			hs.downloadNotFound(w, r)
			return
		}
		return
	}

}
func (hs *HttpServer) checkDownloadAuth(w http.ResponseWriter, r *http.Request) (bool, error) {
	var (
		err          error
		maxTimestamp int64
		minTimestamp int64
		ts           int64
		token        string
		timestamp    string
		fullpath     string
		smallPath    string
		pathMd5      string
		fileInfo     *common.FileInfo
		scene        string
		secret       interface{}
		code         string
		ok           bool
	)
	CheckToken := func(token string, md5sum string, timestamp string) bool {
		if common.Util.MD5(md5sum+timestamp) != token {
			return false
		}
		return true
	}
	if common.Config().EnableDownloadAuth && common.Config().AuthUrl != "" && !common.IsPeer(r) && !hs.server.CheckAuth(w, r) {
		return false, errors.New("auth fail")
	}
	if common.Config().DownloadUseToken && !common.IsPeer(r) {
		token = r.FormValue("token")
		timestamp = r.FormValue("timestamp")
		if token == "" || timestamp == "" {
			return false, errors.New("unvalid request")
		}
		maxTimestamp = time.Now().Add(time.Second *
			time.Duration(common.Config().DownloadTokenExpire)).Unix()
		minTimestamp = time.Now().Add(-time.Second *
			time.Duration(common.Config().DownloadTokenExpire)).Unix()
		if ts, err = strconv.ParseInt(timestamp, 10, 64); err != nil {
			return false, errors.New("unvalid timestamp")
		}
		if ts > maxTimestamp || ts < minTimestamp {
			return false, errors.New("timestamp expire")
		}
		fullpath, smallPath = hs.getFilePathFromRequest(w, r)
		if smallPath != "" {
			pathMd5 = common.Util.MD5(smallPath)
		} else {
			pathMd5 = common.Util.MD5(fullpath)
		}
		if fileInfo, err = hs.server.GetFileInfoFromLevelDB(pathMd5); err != nil {
			// TODO
		} else {
			ok := CheckToken(token, fileInfo.Md5, timestamp)
			if !ok {
				return ok, errors.New("unvalid token")
			}
			return ok, nil
		}
	}
	if common.Config().EnableGoogleAuth && !common.IsPeer(r) {
		fullpath = r.RequestURI[len(common.Config().Group)+2 : len(r.RequestURI)]
		fullpath = strings.Split(fullpath, "?")[0] // just path
		scene = strings.Split(fullpath, "/")[0]
		code = r.FormValue("code")
		if secret, ok = hs.server.sceneMap.GetValue(scene); ok {
			if !hs.server.VerifyGoogleCode(secret.(string), code, int64(common.Config().DownloadTokenExpire/30)) {
				return false, errors.New("invalid google code")
			}
		}
	}
	return true, nil
}
func (hs *HttpServer) downloadSmallFileByURI(w http.ResponseWriter, r *http.Request) (bool, error) {
	var (
		err        error
		data       []byte
		isDownload bool
		imgWidth   int
		imgHeight  int
		width      string
		height     string
		notFound   bool
	)
	r.ParseForm()
	isDownload = true
	if r.FormValue("download") == "" {
		isDownload = common.Config().DefaultDownload
	}
	if r.FormValue("download") == "0" {
		isDownload = false
	}
	width = r.FormValue("width")
	height = r.FormValue("height")
	if width != "" {
		imgWidth, err = strconv.Atoi(width)
		if err != nil {
			log.Error(err)
		}
	}
	if height != "" {
		imgHeight, err = strconv.Atoi(height)
		if err != nil {
			log.Error(err)
		}
	}
	data, notFound, err = hs.getSmallFileByURI(w, r)
	_ = notFound
	if data != nil && string(data[0]) == "1" {
		if isDownload {
			hs.setDownloadHeader(w, r)
		}
		if imgWidth != 0 || imgHeight != 0 {
			hs.resizeImageByBytes(w, data[1:], uint(imgWidth), uint(imgHeight))
			return true, nil
		}
		w.Write(data[1:])
		return true, nil
	}
	return false, errors.New("not found")
}
func (hs *HttpServer) setDownloadHeader(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment")
}
func (hs *HttpServer) getSmallFileByURI(w http.ResponseWriter, r *http.Request) ([]byte, bool, error) {
	var (
		err      error
		data     []byte
		offset   int64
		length   int
		fullpath string
		info     os.FileInfo
	)
	fullpath, _ = hs.getFilePathFromRequest(w, r)
	if _, offset, length, err = hs.parseSmallFile(r.RequestURI); err != nil {
		return nil, false, err
	}
	if info, err = os.Stat(fullpath); err != nil {
		return nil, false, err
	}
	if info.Size() < offset+int64(length) {
		return nil, true, errors.New("noFound")
	} else {
		data, err = common.Util.ReadFileByOffSet(fullpath, offset, length)
		if err != nil {
			return nil, false, err
		}
		return data, false, err
	}
}
func (hs *HttpServer) parseSmallFile(filename string) (string, int64, int, error) {
	var (
		err    error
		offset int64
		length int
	)
	err = errors.New("unvalid small file")
	if len(filename) < 3 {
		return filename, -1, -1, err
	}
	if strings.Contains(filename, "/") {
		filename = filename[strings.LastIndex(filename, "/")+1:]
	}
	pos := strings.Split(filename, ",")
	if len(pos) < 3 {
		return filename, -1, -1, err
	}
	offset, err = strconv.ParseInt(pos[1], 10, 64)
	if err != nil {
		return filename, -1, -1, err
	}
	if length, err = strconv.Atoi(pos[2]); err != nil {
		return filename, offset, -1, err
	}
	if length > cont.CONST_SMALL_FILE_SIZE || offset < 0 {
		err = errors.New("invalid filesize or offset")
		return filename, -1, -1, err
	}
	return pos[0], offset, length, nil
}
func (hs *HttpServer) resizeImageByBytes(w http.ResponseWriter, data []byte, width, height uint) {
	var (
		img     image.Image
		err     error
		imgType string
	)
	reader := bytes.NewReader(data)
	img, imgType, err = image.Decode(reader)
	if err != nil {
		log.Error(err)
		return
	}
	img = resize.Resize(width, height, img, resize.Lanczos3)
	if imgType == "jpg" || imgType == "jpeg" {
		jpeg.Encode(w, img, nil)
	} else if imgType == "png" {
		png.Encode(w, img)
	} else {
		w.Write(data)
	}
}
func (hs *HttpServer) downloadNotFound(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		fullpath   string
		smallPath  string
		isDownload bool
		pathMd5    string
		peer       string
		fileInfo   *common.FileInfo
	)
	fullpath, smallPath = hs.getFilePathFromRequest(w, r)
	isDownload = true
	if r.FormValue("download") == "" {
		isDownload = common.Config().DefaultDownload
	}
	if r.FormValue("download") == "0" {
		isDownload = false
	}
	if smallPath != "" {
		pathMd5 = common.Util.MD5(smallPath)
	} else {
		pathMd5 = common.Util.MD5(fullpath)
	}
	for _, peer = range common.Config().Peers {
		if fileInfo, err = hs.server.checkPeerFileExist(peer, pathMd5, fullpath); err != nil {
			log.Error(err)
			continue
		}
		if fileInfo.Md5 != "" {
			go hs.server.DownloadFromPeer(peer, fileInfo)
			//http.Redirect(w, r, peer+r.RequestURI, 302)
			if isDownload {
				hs.setDownloadHeader(w, r)
			}
			hs.downloadFileToResponse(peer+r.RequestURI, w, r)
			return
		}
	}
	w.WriteHeader(404)
	return
}
func (hs *HttpServer) downloadFileToResponse(url string, w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		req  *httplib.BeegoHTTPRequest
		resp *http.Response
	)
	req = httplib.Get(url)
	req.SetTimeout(time.Second*20, time.Second*600)
	resp, err = req.DoRequest()
	if err != nil {
		log.Error(err)
	}
	defer resp.Body.Close()
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Error(err)
	}
}
func (hs *HttpServer) downloadNormalFileByURI(w http.ResponseWriter, r *http.Request) (bool, error) {
	var (
		err        error
		isDownload bool
		imgWidth   int
		imgHeight  int
		width      string
		height     string
	)
	r.ParseForm()
	isDownload = true
	if r.FormValue("download") == "" {
		isDownload = common.Config().DefaultDownload
	}
	if r.FormValue("download") == "0" {
		isDownload = false
	}
	width = r.FormValue("width")
	height = r.FormValue("height")
	if width != "" {
		imgWidth, err = strconv.Atoi(width)
		if err != nil {
			log.Error(err)
		}
	}
	if height != "" {
		imgHeight, err = strconv.Atoi(height)
		if err != nil {
			log.Error(err)
		}
	}
	if isDownload {
		hs.setDownloadHeader(w, r)
	}
	fullpath, _ := hs.getFilePathFromRequest(w, r)
	if imgWidth != 0 || imgHeight != 0 {
		hs.resizeImage(w, fullpath, uint(imgWidth), uint(imgHeight))
		return true, nil
	}
	staticHandler.ServeHTTP(w, r)
	return true, nil
}
func (hs *HttpServer) resizeImage(w http.ResponseWriter, fullpath string, width, height uint) {
	var (
		img     image.Image
		err     error
		imgType string
		file    *os.File
	)
	file, err = os.Open(fullpath)
	if err != nil {
		log.Error(err)
		return
	}
	img, imgType, err = image.Decode(file)
	if err != nil {
		log.Error(err)
		return
	}
	file.Close()
	img = resize.Resize(width, height, img, resize.Lanczos3)
	if imgType == "jpg" || imgType == "jpeg" {
		jpeg.Encode(w, img, nil)
	} else if imgType == "png" {
		png.Encode(w, img)
	} else {
		file.Seek(0, 0)
		io.Copy(w, file)
	}
}
func (hs *HttpServer) getFilePathFromRequest(w http.ResponseWriter, r *http.Request) (string, string) {
	var (
		err       error
		fullpath  string
		smallPath string
		prefix    string
	)
	fullpath = r.RequestURI[1:]
	if strings.HasPrefix(r.RequestURI, "/"+common.Config().Group+"/") {
		fullpath = r.RequestURI[len(common.Config().Group)+2 : len(r.RequestURI)]
	}
	fullpath = strings.Split(fullpath, "?")[0] // just path
	fullpath = DOCKER_DIR + cont.STORE_DIR_NAME + "/" + fullpath
	prefix = "/" + LARGE_DIR_NAME + "/"
	if common.Config().SupportGroupManage {
		prefix = "/" + common.Config().Group + "/" + LARGE_DIR_NAME + "/"
	}
	if strings.HasPrefix(r.RequestURI, prefix) {
		smallPath = fullpath //notice order
		fullpath = strings.Split(fullpath, ",")[0]
	}
	if fullpath, err = url.PathUnescape(fullpath); err != nil {
		log.Error(err)
	}
	return fullpath, smallPath
}

//
func (hs *HttpServer) Index(w http.ResponseWriter, r *http.Request) {
	var (
		uploadUrl    string
		uploadBigUrl string
		uppy         string
	)
	uploadUrl = "/upload"
	uploadBigUrl = cont.CONST_BIG_UPLOAD_PATH_SUFFIX
	if common.Config().EnableWebUpload {
		if common.Config().SupportGroupManage {
			uploadUrl = fmt.Sprintf("/%s/upload", common.Config().Group)
			uploadBigUrl = fmt.Sprintf("/%s%s", common.Config().Group, cont.CONST_BIG_UPLOAD_PATH_SUFFIX)
		}
		uppy = `<html>
			  
			  <head>
				<meta charset="utf-8" />
				<title>go-fastdfs</title>
				<style>form { bargin } .form-line { display:block;height: 30px;margin:8px; } #stdUpload {background: #fafafa;border-radius: 10px;width: 745px; }</style>
				<link href="https://transloadit.edgly.net/releases/uppy/v0.30.0/dist/uppy.min.css" rel="stylesheet"></head>
			  
			  <body>
                <div>标准上传(强列建议使用这种方式)</div>
				<div id="stdUpload">
				  
				  <form action="%s" method="post" enctype="multipart/form-data">
					<span class="form-line">文件(file):
					  <input type="file" id="file" name="file" /></span>
					<span class="form-line">场景(scene):
					  <input type="text" id="scene" name="scene" value="%s" /></span>
					<span class="form-line">文件名(filename):
					  <input type="text" id="filename" name="filename" value="" /></span>
					<span class="form-line">输出(output):
					  <input type="text" id="output" name="output" value="json" /></span>
					<span class="form-line">自定义路径(path):
					  <input type="text" id="path" name="path" value="" /></span>
	              <span class="form-line">google认证码(code):
					  <input type="text" id="code" name="code" value="" /></span>
					 <span class="form-line">自定义认证(auth_token):
					  <input type="text" id="auth_token" name="auth_token" value="" /></span>
					<input type="submit" name="submit" value="upload" />
                </form>
				</div>
                 <div>断点续传（如果文件很大时可以考虑）</div>
				<div>
				 
				  <div id="drag-drop-area"></div>
				  <script src="https://transloadit.edgly.net/releases/uppy/v0.30.0/dist/uppy.min.js"></script>
				  <script>var uppy = Uppy.Core().use(Uppy.Dashboard, {
					  inline: true,
					  target: '#drag-drop-area'
					}).use(Uppy.Tus, {
					  endpoint: '%s'
					})
					uppy.on('complete', (result) => {
					 // console.log(result) console.log('Upload complete! We’ve uploaded these files:', result.successful)
					})
					//uppy.setMeta({ auth_token: '9ee60e59-cb0f-4578-aaba-29b9fc2919ca',callback_url:'http://127.0.0.1/callback' ,filename:'自定义文件名','path':'自定义path',scene:'自定义场景' })//这里是传递上传的认证参数,callback_url参数中 id为文件的ID,info 文转的基本信息json
					uppy.setMeta({ auth_token: '9ee60e59-cb0f-4578-aaba-29b9fc2919ca',callback_url:'http://127.0.0.1/callback'})//自定义参数与普通上传类似（虽然支持自定义，建议不要自定义，海量文件情况下，自定义很可能给自已给埋坑）
                </script>
				</div>
			  </body>
			</html>`
		uppyFileName := STATIC_DIR + "/uppy.html"
		if common.Util.IsExist(uppyFileName) {
			if data, err := common.Util.ReadBinFile(uppyFileName); err != nil {
				log.Error(err)
			} else {
				uppy = string(data)
			}
		} else {
			common.Util.WriteFile(uppyFileName, uppy)
		}
		fmt.Fprintf(w,
			fmt.Sprintf(uppy, uploadUrl, common.Config().DefaultScene, uploadBigUrl))
	} else {
		w.Write([]byte("web upload deny"))
	}
}

//
func (hs *HttpServer) CheckFilesExist(w http.ResponseWriter, r *http.Request) {
	var (
		data      []byte
		err       error
		fileInfo  *common.FileInfo
		fileInfos []*common.FileInfo
		fpath     string
		result    common.JsonResult
	)
	r.ParseForm()
	md5sum := ""
	md5sum = r.FormValue("md5s")
	md5s := strings.Split(md5sum, ",")
	for _, m := range md5s {
		if fileInfo, err = hs.server.GetFileInfoFromLevelDB(m); fileInfo != nil {
			if fileInfo.OffSet != -1 {
				if data, err = common.JSON.Marshal(fileInfo); err != nil {
					log.Error(err)
				}
				//w.Write(data)
				//return
				fileInfos = append(fileInfos, fileInfo)
				continue
			}
			fpath = DOCKER_DIR + fileInfo.Path + "/" + fileInfo.Name
			if fileInfo.ReName != "" {
				fpath = DOCKER_DIR + fileInfo.Path + "/" + fileInfo.ReName
			}
			if common.Util.IsExist(fpath) {
				if data, err = common.JSON.Marshal(fileInfo); err == nil {
					fileInfos = append(fileInfos, fileInfo)
					//w.Write(data)
					//return
					continue
				} else {
					log.Error(err)
				}
			} else {
				if fileInfo.OffSet == -1 {
					hs.server.RemoveKeyFromLevelDB(md5sum, hs.server.ldb) // when file delete,delete from leveldb
				}
			}
		}
	}
	result.Data = fileInfos
	data, _ = common.JSON.Marshal(result)
	w.Write(data)
	return
}

//
func (hs *HttpServer) CheckFileExist(w http.ResponseWriter, r *http.Request) {
	var (
		data     []byte
		err      error
		fileInfo *common.FileInfo
		fpath    string
		fi       os.FileInfo
	)
	r.ParseForm()
	md5sum := ""
	md5sum = r.FormValue("md5")
	fpath = r.FormValue("path")
	if fileInfo, err = hs.server.GetFileInfoFromLevelDB(md5sum); fileInfo != nil {
		if fileInfo.OffSet != -1 {
			if data, err = common.JSON.Marshal(fileInfo); err != nil {
				log.Error(err)
			}
			w.Write(data)
			return
		}
		fpath = DOCKER_DIR + fileInfo.Path + "/" + fileInfo.Name
		if fileInfo.ReName != "" {
			fpath = DOCKER_DIR + fileInfo.Path + "/" + fileInfo.ReName
		}
		if common.Util.IsExist(fpath) {
			if data, err = common.JSON.Marshal(fileInfo); err == nil {
				w.Write(data)
				return
			} else {
				log.Error(err)
			}
		} else {
			if fileInfo.OffSet == -1 {
				hs.server.RemoveKeyFromLevelDB(md5sum, hs.server.ldb) // when file delete,delete from leveldb
			}
		}
	} else {
		if fpath != "" {
			fi, err = os.Stat(fpath)
			if err == nil {
				sum := common.Util.MD5(fpath)
				//if Config().EnableDistinctFile {
				//	sum, err = common.Util.GetFileSumByName(fpath, Config().FileSumArithmetic)
				//	if err != nil {
				//		log.Error(err)
				//	}
				//}
				fileInfo = &common.FileInfo{
					Path:      path.Dir(fpath),
					Name:      path.Base(fpath),
					Size:      fi.Size(),
					Md5:       sum,
					Peers:     []string{common.Config().Host},
					OffSet:    -1, //very important
					TimeStamp: fi.ModTime().Unix(),
				}
				data, err = common.JSON.Marshal(fileInfo)
				w.Write(data)
				return
			}
		}
	}
	data, _ = common.JSON.Marshal(common.FileInfo{})
	w.Write(data)
	return
}

//
func (hs *HttpServer) Upload(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		fn     string
		folder string
		fpTmp  *os.File
		fpBody *os.File
	)
	if r.Method == http.MethodGet {
		hs.server.upload(w, r)
		return
	}
	folder = STORE_DIR + "/_tmp/" + time.Now().Format("20060102")
	os.MkdirAll(folder, 0777)
	fn = folder + "/" + common.Util.GetUUID()
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
	hs.server.queueUpload <- common.WrapReqResp{&w, r, done}
	<-done

}

//
func (hs *HttpServer) RemoveFile(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		md5sum   string
		fileInfo *common.FileInfo
		fpath    string
		delUrl   string
		result   common.JsonResult
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
	if !common.IsPeer(r) {
		w.Write([]byte(common.GetClusterNotPermitMessage(r)))
		return
	}
	if common.Config().AuthUrl != "" && !hs.server.CheckAuth(w, r) {
		hs.server.NotPermit(w, r)
		return
	}
	if fpath != "" && md5sum == "" {
		fpath = strings.Replace(fpath, "/"+common.Config().Group+"/", cont.STORE_DIR_NAME+"/", 1)
		md5sum = common.Util.MD5(fpath)
	}
	if inner != "1" {
		for _, peer := range common.Config().Peers {
			delFile := func(peer string, md5sum string, fileInfo *common.FileInfo) {
				delUrl = fmt.Sprintf("%s%s", peer, hs.server.getRequestURI("delete"))
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
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	if fileInfo, err = hs.server.GetFileInfoFromLevelDB(md5sum); err != nil {
		result.Message = err.Error()
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	if fileInfo.OffSet >= 0 {
		result.Message = "small file delete not support"
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	name = fileInfo.Name
	if fileInfo.ReName != "" {
		name = fileInfo.ReName
	}
	fpath = fileInfo.Path + "/" + name
	if fileInfo.Path != "" && common.Util.FileExists(DOCKER_DIR+fpath) {
		hs.server.SaveFileMd5Log(fileInfo, cont.CONST_REMOME_Md5_FILE_NAME)
		if err = os.Remove(DOCKER_DIR + fpath); err != nil {
			result.Message = err.Error()
			w.Write([]byte(common.Util.JsonEncodePretty(result)))
			return
		} else {
			result.Message = "remove success"
			result.Status = "ok"
			w.Write([]byte(common.Util.JsonEncodePretty(result)))
			return
		}
	}
	result.Message = "fail remove"
	w.Write([]byte(common.Util.JsonEncodePretty(result)))
}

//
func (hs *HttpServer) GetFileInfo(w http.ResponseWriter, r *http.Request) {
	var (
		fpath    string
		md5sum   string
		fileInfo *common.FileInfo
		err      error
		result   common.JsonResult
	)
	md5sum = r.FormValue("md5")
	fpath = r.FormValue("path")
	result.Status = "fail"
	if !common.IsPeer(r) {
		w.Write([]byte(common.GetClusterNotPermitMessage(r)))
		return
	}
	md5sum = r.FormValue("md5")
	if fpath != "" {
		fpath = strings.Replace(fpath, "/"+common.Config().Group+"/", cont.STORE_DIR_NAME+"/", 1)
		md5sum = common.Util.MD5(fpath)
	}
	if fileInfo, err = hs.server.GetFileInfoFromLevelDB(md5sum); err != nil {
		log.Error(err)
		result.Message = err.Error()
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	result.Status = "ok"
	result.Data = fileInfo
	w.Write([]byte(common.Util.JsonEncodePretty(result)))
	return
}

//
func (hs *HttpServer) Sync(w http.ResponseWriter, r *http.Request) {
	var (
		result common.JsonResult
	)
	r.ParseForm()
	result.Status = "fail"
	if !common.IsPeer(r) {
		result.Message = "client must be in cluster"
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
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
		for _, peer := range common.Config().Peers {
			req := httplib.Post(peer + hs.server.getRequestURI("sync"))
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
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	date = strings.Replace(date, ".", "", -1)
	if isForceUpload {
		go hs.server.checkFileAndSendToPeer(date, cont.CONST_FILE_Md5_FILE_NAME, isForceUpload)
	} else {
		go hs.server.checkFileAndSendToPeer(date, cont.CONST_Md5_ERROR_FILE_NAME, isForceUpload)
	}
	result.Status = "ok"
	result.Message = "job is running"
	w.Write([]byte(common.Util.JsonEncodePretty(result)))
}

//
func (hs *HttpServer) Stat(w http.ResponseWriter, r *http.Request) {
	var (
		result   common.JsonResult
		inner    string
		echart   string
		category []string
		barCount []int64
		barSize  []int64
		dataMap  map[string]interface{}
	)
	if !common.IsPeer(r) {
		result.Message = common.GetClusterNotPermitMessage(r)
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	r.ParseForm()
	inner = r.FormValue("inner")
	echart = r.FormValue("echart")
	data := hs.getStat()
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
		w.Write([]byte(common.Util.JsonEncodePretty(data)))
	} else {
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
	}
}

//
func (hs *HttpServer) Status(w http.ResponseWriter, r *http.Request) {
	var (
		status   common.JsonResult
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
	today = common.Util.GetToDay()
	sts = make(map[string]interface{})
	sts["Fs.QueueFromPeers"] = len(hs.server.queueFromPeers)
	sts["Fs.QueueToPeers"] = len(hs.server.queueToPeers)
	sts["Fs.QueueFileLog"] = len(hs.server.queueFileLog)
	for _, k := range []string{cont.CONST_FILE_Md5_FILE_NAME, cont.CONST_Md5_ERROR_FILE_NAME, cont.CONST_Md5_QUEUE_FILE_NAME} {
		k2 := fmt.Sprintf("%s_%s", today, k)
		if v, ok = hs.server.sumMap.GetValue(k2); ok {
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
	sts["Fs.AutoRepair"] = common.Config().AutoRepair
	sts["Fs.QueueUpload"] = len(hs.server.queueUpload)
	sts["Fs.RefreshInterval"] = common.Config().RefreshInterval
	sts["Fs.Peers"] = common.Config().Peers
	sts["Fs.Local"] = hs.server.host
	sts["Fs.FileStats"] = hs.getStat()
	sts["Fs.ShowDir"] = common.Config().ShowDir
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
	w.Write([]byte(common.Util.JsonEncodePretty(status)))
}
func (hs *HttpServer) getStat() []common.StatDateFileInfo {
	var (
		min   int64
		max   int64
		err   error
		i     int64
		rows  []common.StatDateFileInfo
		total common.StatDateFileInfo
	)
	min = 20190101
	max = 20190101
	for k := range hs.server.statMap.Get() {
		ks := strings.Split(k, "_")
		if len(ks) == 2 {
			if i, err = strconv.ParseInt(ks[0], 10, 64); err != nil {
				continue
			}
			if i >= max {
				max = i
			}
			if i < min {
				min = i
			}
		}
	}
	for i := min; i <= max; i++ {
		s := fmt.Sprintf("%d", i)
		if v, ok := hs.server.statMap.GetValue(s + "_" + cont.CONST_STAT_FILE_TOTAL_SIZE_KEY); ok {
			var info common.StatDateFileInfo
			info.Date = s
			switch v.(type) {
			case int64:
				info.TotalSize = v.(int64)
				total.TotalSize = total.TotalSize + v.(int64)
			}
			if v, ok := hs.server.statMap.GetValue(s + "_" + cont.CONST_STAT_FILE_COUNT_KEY); ok {
				switch v.(type) {
				case int64:
					info.FileCount = v.(int64)
					total.FileCount = total.FileCount + v.(int64)
				}
			}
			rows = append(rows, info)
		}
	}
	total.Date = "all"
	rows = append(rows, total)
	return rows
}

//
func (hs *HttpServer) RepairStatWeb(w http.ResponseWriter, r *http.Request) {
	var (
		result common.JsonResult
		date   string
		inner  string
	)
	if !common.IsPeer(r) {
		result.Message = common.GetClusterNotPermitMessage(r)
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	date = r.FormValue("date")
	inner = r.FormValue("inner")
	if ok, err := regexp.MatchString("\\d{8}", date); err != nil || !ok {
		result.Message = "invalid date"
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	if date == "" || len(date) != 8 {
		date = common.Util.GetToDay()
	}
	if inner != "1" {
		for _, peer := range common.Config().Peers {
			req := httplib.Post(peer + hs.server.getRequestURI("repair_stat"))
			req.Param("inner", "1")
			req.Param("date", date)
			if _, err := req.String(); err != nil {
				log.Error(err)
			}
		}
	}
	result.Data = hs.server.repairStatByDate(date)
	result.Status = "ok"
	w.Write([]byte(common.Util.JsonEncodePretty(result)))
}

//
func (hs *HttpServer) Repair(w http.ResponseWriter, r *http.Request) {
	var (
		force       string
		forceRepair bool
		result      common.JsonResult
	)
	result.Status = "ok"
	r.ParseForm()
	force = r.FormValue("force")
	if force == "1" {
		forceRepair = true
	}
	if common.IsPeer(r) {
		go hs.server.autoRepair(forceRepair)
		result.Message = "repair job start..."
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
	} else {
		result.Message = common.GetClusterNotPermitMessage(r)
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
	}

}

//
func (hs *HttpServer) Report(w http.ResponseWriter, r *http.Request) {
	var (
		reportFileName string
		result         common.JsonResult
		html           string
	)
	result.Status = "ok"
	r.ParseForm()
	if common.IsPeer(r) {
		reportFileName = STATIC_DIR + "/report.html"
		if common.Util.IsExist(reportFileName) {
			if data, err := common.Util.ReadBinFile(reportFileName); err != nil {
				log.Error(err)
				result.Message = err.Error()
				w.Write([]byte(common.Util.JsonEncodePretty(result)))
				return
			} else {
				html = string(data)
				if common.Config().SupportGroupManage {
					html = strings.Replace(html, "{group}", "/"+common.Config().Group, 1)
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
		w.Write([]byte(common.GetClusterNotPermitMessage(r)))
	}
}

//
func (hs *HttpServer) BackUp(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		date   string
		result common.JsonResult
		inner  string
		url    string
	)
	result.Status = "ok"
	r.ParseForm()
	date = r.FormValue("date")
	inner = r.FormValue("inner")
	if date == "" {
		date = common.Util.GetToDay()
	}
	if common.IsPeer(r) {
		if inner != "1" {
			for _, peer := range common.Config().Peers {
				backUp := func(peer string, date string) {
					url = fmt.Sprintf("%s%s", peer, hs.server.getRequestURI("backup"))
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
		go hs.server.BackUpMetaDataByDate(date)
		result.Message = "back job start..."
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
	} else {
		result.Message = common.GetClusterNotPermitMessage(r)
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
	}
}

//
func (hs *HttpServer) Search(w http.ResponseWriter, r *http.Request) {
	var (
		result    common.JsonResult
		err       error
		kw        string
		count     int
		fileInfos []common.FileInfo
		md5s      []string
	)
	kw = r.FormValue("kw")
	if !common.IsPeer(r) {
		result.Message = common.GetClusterNotPermitMessage(r)
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	iter := hs.server.ldb.NewIterator(nil, nil)
	for iter.Next() {
		var fileInfo common.FileInfo
		value := iter.Value()
		if err = common.JSON.Unmarshal(value, &fileInfo); err != nil {
			log.Error(err)
			continue
		}
		if strings.Contains(fileInfo.Name, kw) && !common.Util.Contains(fileInfo.Md5, md5s) {
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
	//fileInfos=hs.SearchDict(kw) // serch file from map for huge capacity
	result.Status = "ok"
	result.Data = fileInfos
	w.Write([]byte(common.Util.JsonEncodePretty(result)))
}

//
func (hs *HttpServer) ListDir(w http.ResponseWriter, r *http.Request) {
	var (
		result      common.JsonResult
		dir         string
		filesInfo   []os.FileInfo
		err         error
		filesResult []common.FileInfoResult
		tmpDir      string
	)
	if !common.IsPeer(r) {
		result.Message = common.GetClusterNotPermitMessage(r)
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	dir = r.FormValue("dir")
	//if dir == "" {
	//	result.Message = "dir can't null"
	//	w.Write([]byte(common.Util.JsonEncodePretty(result)))
	//	return
	//}
	dir = strings.Replace(dir, ".", "", -1)
	if tmpDir, err = os.Readlink(dir); err == nil {
		dir = tmpDir
	}
	filesInfo, err = ioutil.ReadDir(DOCKER_DIR + cont.STORE_DIR_NAME + "/" + dir)
	if err != nil {
		log.Error(err)
		result.Message = err.Error()
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	for _, f := range filesInfo {
		fi := common.FileInfoResult{
			Name:    f.Name(),
			Size:    f.Size(),
			IsDir:   f.IsDir(),
			ModTime: f.ModTime().Unix(),
			Path:    dir,
			Md5:     common.Util.MD5(strings.Replace(cont.STORE_DIR_NAME+"/"+dir+"/"+f.Name(), "//", "/", -1)),
		}
		filesResult = append(filesResult, fi)
	}
	result.Status = "ok"
	result.Data = filesResult
	w.Write([]byte(common.Util.JsonEncodePretty(result)))
	return
}

//
func (hs *HttpServer) RemoveEmptyDir(w http.ResponseWriter, r *http.Request) {
	var (
		result common.JsonResult
	)
	result.Status = "ok"
	if common.IsPeer(r) {
		go common.Util.RemoveEmptyDir(DATA_DIR)
		go common.Util.RemoveEmptyDir(STORE_DIR)
		result.Message = "clean job start ..,don't try again!!!"
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
	} else {
		result.Message = common.GetClusterNotPermitMessage(r)
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
	}
}

//
func (hs *HttpServer) RepairFileInfo(w http.ResponseWriter, r *http.Request) {
	var (
		result common.JsonResult
	)
	if !common.IsPeer(r) {
		w.Write([]byte(common.GetClusterNotPermitMessage(r)))
		return
	}
	if !common.Config().EnableMigrate {
		w.Write([]byte("please set enable_migrate=true"))
		return
	}
	result.Status = "ok"
	result.Message = "repair job start,don't try again,very danger "
	go hs.server.repairFileInfoFromFile()
	w.Write([]byte(common.Util.JsonEncodePretty(result)))
}

//
func (hs *HttpServer) SyncFileInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err         error
		fileInfo    common.FileInfo
		fileInfoStr string
		filename    string
	)
	r.ParseForm()
	if !common.IsPeer(r) {
		return
	}
	fileInfoStr = r.FormValue("fileInfo")
	if err = common.JSON.Unmarshal([]byte(fileInfoStr), &fileInfo); err != nil {
		w.Write([]byte(common.GetClusterNotPermitMessage(r)))
		log.Error(err)
		return
	}
	if fileInfo.OffSet == -2 {
		// optimize migrate
		hs.server.SaveFileInfoToLevelDB(fileInfo.Md5, &fileInfo, hs.server.ldb)
	} else {
		hs.server.SaveFileMd5Log(&fileInfo, cont.CONST_Md5_QUEUE_FILE_NAME)
	}
	hs.server.AppendToDownloadQueue(&fileInfo)
	filename = fileInfo.Name
	if fileInfo.ReName != "" {
		filename = fileInfo.ReName
	}
	p := strings.Replace(fileInfo.Path, STORE_DIR+"/", "", 1)
	downloadUrl := fmt.Sprintf("http://%s/%s", r.Host, common.Config().Group+"/"+p+"/"+filename)
	log.Info("SyncFileInfo: ", downloadUrl)
	w.Write([]byte(downloadUrl))
}

//
func (hs *HttpServer) GetMd5sForWeb(w http.ResponseWriter, r *http.Request) {
	var (
		date   string
		err    error
		result mapset.Set
		lines  []string
		md5s   []interface{}
	)
	if !common.IsPeer(r) {
		w.Write([]byte(common.GetClusterNotPermitMessage(r)))
		return
	}
	date = r.FormValue("date")
	if result, err = hs.server.GetMd5sByDate(date, cont.CONST_FILE_Md5_FILE_NAME); err != nil {
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

//
func (hs *HttpServer) ReceiveMd5s(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		md5str   string
		fileInfo *common.FileInfo
		md5s     []string
	)
	if !common.IsPeer(r) {
		log.Warn(fmt.Sprintf("ReceiveMd5s %s", common.Util.GetClientIp(r)))
		w.Write([]byte(common.GetClusterNotPermitMessage(r)))
		return
	}
	r.ParseForm()
	md5str = r.FormValue("md5s")
	md5s = strings.Split(md5str, ",")
	AppendFunc := func(md5s []string) {
		for _, m := range md5s {
			if m != "" {
				if fileInfo, err = hs.server.GetFileInfoFromLevelDB(m); err != nil {
					log.Error(err)
					continue
				}
				hs.server.AppendToQueue(fileInfo)
			}
		}
	}
	go AppendFunc(md5s)
}

//
func (hs *HttpServer) GenGoogleSecret(w http.ResponseWriter, r *http.Request) {
	var (
		result common.JsonResult
	)
	result.Status = "ok"
	result.Message = "ok"
	if !common.IsPeer(r) {
		result.Message = common.GetClusterNotPermitMessage(r)
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
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
	w.Write([]byte(common.Util.JsonEncodePretty(result)))
}

//
func (hs *HttpServer) GenGoogleCode(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		result common.JsonResult
		secret string
		goauth *googleAuthenticator.GAuth
	)
	r.ParseForm()
	goauth = googleAuthenticator.NewGAuth()
	secret = r.FormValue("secret")
	result.Status = "ok"
	result.Message = "ok"
	if !common.IsPeer(r) {
		result.Message = common.GetClusterNotPermitMessage(r)
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	if result.Data, err = goauth.GetCode(secret); err != nil {
		result.Message = err.Error()
		w.Write([]byte(common.Util.JsonEncodePretty(result)))
		return
	}
	w.Write([]byte(common.Util.JsonEncodePretty(result)))
}
