package config

import (
	"fmt"
	"github.com/thinxz-yuan/go-fastdfs/common"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync/atomic"
	"unsafe"

	_ "github.com/eventials/go-tus"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sjqzhang/seelog"
	"github.com/thinxz-yuan/go-fastdfs/serv/cont"
)

var (
	ptr  unsafe.Pointer
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

var (
	FileName string
)

type GlobalConfig struct {
	Addr                 string      `json:"addr"`
	Peers                []string    `json:"peers"`
	Group                string      `json:"group"`
	RenameFile           bool        `json:"rename_file"`
	ShowDir              bool        `json:"show_dir"`
	Extensions           []string    `json:"extensions"`
	RefreshInterval      int         `json:"refresh_interval"`
	EnableWebUpload      bool        `json:"enable_web_upload"`
	DownloadDomain       string      `json:"download_domain"`
	EnableCustomPath     bool        `json:"enable_custom_path"`
	Scenes               []string    `json:"scenes"`
	AlarmReceivers       []string    `json:"alarm_receivers"`
	DefaultScene         string      `json:"default_scene"`
	Mail                 common.Mail `json:"mail"`
	AlarmUrl             string      `json:"alarm_url"`
	DownloadUseToken     bool        `json:"download_use_token"`
	DownloadTokenExpire  int         `json:"download_token_expire"`
	QueueSize            int         `json:"queue_size"`
	AutoRepair           bool        `json:"auto_repair"`
	Host                 string      `json:"host"`
	FileSumArithmetic    string      `json:"file_sum_arithmetic"`
	PeerId               string      `json:"peer_id"`
	SupportGroupManage   bool        `json:"support_group_manage"`
	AdminIps             []string    `json:"admin_ips"`
	EnableMergeSmallFile bool        `json:"enable_merge_small_file"`
	EnableMigrate        bool        `json:"enable_migrate"`
	EnableDistinctFile   bool        `json:"enable_distinct_file"`
	ReadOnly             bool        `json:"read_only"`
	EnableCrossOrigin    bool        `json:"enable_cross_origin"`
	EnableGoogleAuth     bool        `json:"enable_google_auth"`
	AuthUrl              string      `json:"auth_url"`
	EnableDownloadAuth   bool        `json:"enable_download_auth"`
	DefaultDownload      bool        `json:"default_download"`
	EnableTus            bool        `json:"enable_tus"`
	SyncTimeout          int64       `json:"sync_timeout"`
	EnableFsnotify       bool        `json:"enable_fsnotify"`
	EnableDiskCache      bool        `json:"enable_disk_cache"`
	ConnectTimeout       bool        `json:"connect_timeout"`
	ReadTimeout          int         `json:"read_timeout"`
	WriteTimeout         int         `json:"write_timeout"`
	IdleTimeout          int         `json:"idle_timeout"`
	ReadHeaderTimeout    int         `json:"read_header_timeout"`
	SyncWorker           int         `json:"sync_worker"`
	UploadWorker         int         `json:"upload_worker"`
	UploadQueueSize      int         `json:"upload_queue_size"`
	RetryCount           int         `json:"retry_count"`
}

func Config() *GlobalConfig {
	return (*GlobalConfig)(atomic.LoadPointer(&ptr))
}

func ParseConfig(filePath string) {
	var data []byte
	if filePath == "" {
		//
		data = []byte(strings.TrimSpace(cont.CfgJson))
	} else {
		//
		file, err := os.Open(filePath)
		if err != nil {
			panic(fmt.Sprintln("open file path:", filePath, "error:", err))
		}
		defer file.Close()
		FileName = filePath
		data, err = ioutil.ReadAll(file)
		if err != nil {
			panic(fmt.Sprintln("file path:", filePath, " read all error:", err))
		}
	}

	//
	var c GlobalConfig
	if err := json.Unmarshal(data, &c); err != nil {
		panic(fmt.Sprintln("file path:", filePath, "json unmarshal error:", err))
	}

	//
	log.Info(c)
	atomic.StorePointer(&ptr, unsafe.Pointer(&c))
	log.Info("config parse success")
}
