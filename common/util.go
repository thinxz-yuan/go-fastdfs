package common

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/sjqzhang/goutil"
	"github.com/thinxz-yuan/go-fastdfs/serv/cont"
	"net/http"
	"os"
	"strings"
)

var (
	JSON = jsoniter.ConfigCompatibleWithStandardLibrary
	Util = &goutil.Common{}
)

//
func CrossOrigin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, X-File-Type, Cache-Control, Origin")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Expose-Headers", "Authorization")
	//https://blog.csdn.net/yanzisu_congcong/article/details/80552155
}

func GetClusterNotPermitMessage(r *http.Request) string {
	var (
		message string
	)
	message = fmt.Sprintf(cont.CONST_MESSAGE_CLUSTER_IP, Util.GetClientIp(r))
	return message
}

func IsPeer(r *http.Request) bool {
	var (
		ip    string
		peer  string
		bflag bool
	)
	//return true
	ip = Util.GetClientIp(r)
	realIp := os.Getenv("GO_FASTDFS_IP")
	if realIp == "" {
		realIp = Util.GetPulicIP()
	}
	if ip == "127.0.0.1" || ip == realIp {
		return true
	}
	if Util.Contains(ip, Config().AdminIps) {
		return true
	}
	ip = "http://" + ip
	bflag = false
	for _, peer = range Config().Peers {
		if strings.HasPrefix(peer, ip) {
			bflag = true
			break
		}
	}
	return bflag
}
