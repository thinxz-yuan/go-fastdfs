package serv

import (
	"errors"
	"fmt"
	"github.com/astaxie/beego/httplib"
	_ "github.com/eventials/go-tus"
	log "github.com/sjqzhang/seelog"
	"github.com/sjqzhang/tusd"
	"github.com/thinxz-yuan/go-fastdfs/common"
	_ "net/http/pprof"
	"strings"
	"time"
)

type HookDataStore struct {
	tusd.DataStore
}

func (store HookDataStore) NewUpload(info tusd.FileInfo) (id string, err error) {
	var (
		jsonResult common.JsonResult
	)
	if common.Config().AuthUrl != "" {
		if auth_token, ok := info.MetaData["auth_token"]; !ok {
			msg := "token auth fail,auth_token is not in http header Upload-Metadata," +
				"in uppy uppy.setMeta({ auth_token: '9ee60e59-cb0f-4578-aaba-29b9fc2919ca' })"
			log.Error(msg, fmt.Sprintf("current header:%v", info.MetaData))
			return "", HttpError{error: errors.New(msg), statusCode: 401}
		} else {
			req := httplib.Post(common.Config().AuthUrl)
			req.Param("auth_token", auth_token)
			req.SetTimeout(time.Second*5, time.Second*10)
			content, err := req.String()
			content = strings.TrimSpace(content)
			if strings.HasPrefix(content, "{") && strings.HasSuffix(content, "}") {
				if err = json.Unmarshal([]byte(content), &jsonResult); err != nil {
					log.Error(err)
					return "", HttpError{error: errors.New(err.Error() + content), statusCode: 401}
				}
				if jsonResult.Data != "ok" {
					return "", HttpError{error: errors.New(content), statusCode: 401}
				}
			} else {
				if err != nil {
					log.Error(err)
					return "", err
				}
				if strings.TrimSpace(content) != "ok" {
					return "", HttpError{error: errors.New(content), statusCode: 401}
				}
			}
		}
	}
	return store.DataStore.NewUpload(info)
}

type HttpError struct {
	error
	statusCode int
}

func (err HttpError) StatusCode() int {
	return err.statusCode
}

func (err HttpError) Body() []byte {
	return []byte(err.Error())
}
