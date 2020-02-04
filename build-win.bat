del fileserver
del file_server
set GOPATH=E:\\co\\go
set GOOS=linux
set GOARCH=amd64

go build -o file_server ^
-ldflags "-w -s -X 'main.VERSION=$BIN_VERSION' -X 'main.BUILD_TIME=build_time:`date`' -X 'main.GO_VERSION=`go version`' -X 'main.GIT_VERSION=git_version:`git rev-parse HEAD`'" fileserver.go

nohup file_server > run.log 2>&1 &