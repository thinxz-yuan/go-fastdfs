package main

import (
	"../go-fastdfs/man"
)

func main() {

	m := man.Manager{}
	m.Init()
	m.InitStitch()
	m.Start()

	// 初始化, 所有元器对象 -> 并填充默认值(静态属性)
	//r1 := rel.R{}
	//r1.Init("R1")
	//r2 := rel.R{}
	//r2.Init("R2")
	//d1 := rel.D{}
	//d1.Init("D1")
	//
	//// 初始化, 针脚连接
	//r1.AddStitch(1, r2.Component, 2)
	//r1.AddStitch(2, d1.Component, 2)
	//
	//r2.AddStitch(1, d1.Component, 1)
	//r2.AddStitch(2, r1.Component, 1)
	//
	//d1.AddStitch(1, r2.Component, 1)
	//d1.AddStitch(2, r1.Component, 2)

	// 数据针脚描述信息
	//fmt.Println(r1.Describe())
	//fmt.Println(r2.Describe())
	//fmt.Println(d1.Describe())

	//fmt.Println()
	//
	//// 电源初始化计算
	//d1.InitCalculate()
}
