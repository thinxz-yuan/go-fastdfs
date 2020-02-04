package main

import (
	"github.com/thinxz-yuan/go-fastdfs/serv"
)

func main() {
	s := serv.Start()

	serv.NewHttpServer(s, s.GroupName())
}
