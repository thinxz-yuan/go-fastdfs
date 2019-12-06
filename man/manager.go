package man

import (
	"../event"
	"../linked"
	"../rel"
	"fmt"
)

var (
	r1 rel.R
	r2 rel.R
	d1 rel.D
)

type Manager struct {
	events linked.List
}

func (m *Manager) init() {
	// 初始化, 所有元器对象 -> 并填充默认值(静态属性)
	r1 = rel.R{}
	r1.Init("R1", m)
	r2 = rel.R{}
	r2.Init("R2", m)
	d1 = rel.D{}
	d1.Init("D1", m)
}

func (m *Manager) initStitch() {
	// 初始化, 针脚连接
	r1.AddStitch(1, r2.Component, 2)
	r1.AddStitch(2, d1.Component, 2)

	r2.AddStitch(1, d1.Component, 1)
	r2.AddStitch(2, r1.Component, 1)

	d1.AddStitch(1, r2.Component, 1)
	d1.AddStitch(2, r1.Component, 2)
}

func (m *Manager) Start() {
	// 初始化并创建, 所有器件对象
	m.init()
	// 初始化, 所有器件的针脚数据
	m.initStitch()

	fmt.Println()
	// 加电自检电源初始化计算
	d1.InitCalculate()

	// 循环处理事件
	fmt.Println()
}

func (m *Manager) PutEvent(event *event.Event) {
	m.events.Put(event)
}
