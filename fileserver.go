package main

import (
	"github.com/thinxz-yuan/go-fastdfs/serv"
	"github.com/thinxz-yuan/go-fastdfs/serv/web"
)

func main() {
	s := serv.Start()

	web.StartHttpServe(s, "")
}
