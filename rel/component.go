package rel

import (
	"../event"
	"../man"
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

//// 器件定义
//// ---------- ---------
//type Component interface {
//	// 器件初始化
//	Init()
//	// 添加针脚
//	AddStitch(no int, target Component, targetNo int)
//	// 重新计算内部属性
//	Calculate()
//	// 发布事件
//	Event(event string)
//
//	// 遍历针脚, 填充值
//
//	// 针脚操作
//
//	// 计算输出值
//
//	// 器件名称
//	Name() string
//	// 器件描述
//	Describe() string
//}

type Component struct {
	// 器件名称
	name string
	// 针脚定义
	S       map[int]Stitch
	manager *man.Manager
}

// 初始化器件数据
// ---------- ----------
func (c *Component) Init(name string, manager *man.Manager) {
	c.S = make(map[int]Stitch)
	c.name = name
	c.manager = manager
}

// 获取器件名称, 唯一标识符
// ---------- ----------
func (c *Component) Name() string {
	return c.name
}

// 初始化器件针脚, 及其关联器件针脚
// ---------- ----------
func (c *Component) AddStitch(no int, target Component, targetNo int) {
	s, ok := c.S[no]
	if !ok {
		s = Stitch{
			no:       no,
			Relation: []*Relation{},
		}
	}

	s.AddRelation(&Relation{
		No:        targetNo,
		Component: target,
	})

	c.S[no] = s
}

// 信息传入接口
// ---------- ----------
// data 传入该器件的信息内容
// no   事件触发的针脚号
// ---------- ----------
func (c *Component) Write(data string, no int) (err error) {
	_, ok := c.S[no]
	if !ok {
		return NewErr("针脚号错误, 已关闭或不存在")
	}
	return
}

func (c *Component) InitCalculate() {
	//c.calculate()
}

func (c *Component) calculate() {
	// 计算完毕, 信息传递
	fmt.Println(fmt.Sprintf("信息传递 ing ... -> Component [%s] ", c.name))
	fmt.Println()
	// 遍历针脚数据, 并传递针脚计算值
	for _, v := range c.S {
		for i := 0; i < len(v.Relation); i++ {
			// 器件 -> 关联其他器件针脚
			no := v.Relation[i].No
			// v.Relation[i].Component
			// 01 传递针脚数据
			err := v.Relation[i].Component.Write("", no)
			if err != nil {
				fmt.Println(fmt.Sprintf("%s:%d -> %s", v.Relation[i].Component.Name(), no, err.Error()))
				//

			}

			// 02 触发关联器件事件 [传递触发的针脚号]
			v.Relation[i].Component.Event("", no)
		}
	}
}

// 器件描述信息, 返回JSON格式描述
// ---------- ----------
// event 事件类型定义
// no    事件触发的针脚号
// ---------- ----------
func (c *Component) Event(event string, no int) {
	fmt.Println(fmt.Sprintf("接收事件, Component [%s:%d] Event [%s] ... ", c.name, no, event))
	// 发布事件到事件管理器 -> [事件管理器根据器件唯一标识符查询器件, 并触发相联器件计算]

	&event.Event{}

	c.manager.PutEvent(e)
}

// 器件描述信息, 返回JSON格式描述
// ---------- ----------
func (c *Component) Describe() string {
	var buff bytes.Buffer
	for _, v := range c.S {
		var buffer bytes.Buffer
		// 针脚编号 strconv.FormatInt(, )
		buffer.WriteString(fmt.Sprintf("\"%d\":[", v.GetNo()))
		for i := 0; i < len(v.Relation); i++ {
			// 器件名称:器件针脚编号,
			buffer.WriteString("\"")
			buffer.WriteString(v.Relation[i].Component.Name())
			buffer.WriteString("\"")
			buffer.WriteString(":")
			buffer.WriteString(strconv.Itoa(v.Relation[i].No))
			buffer.WriteString(",")
		}
		buffer.WriteString("]")
		// 关联器件名称, 关联器件编号
		buff.WriteString(fmt.Sprintf("%s],", strings.TrimRight(buffer.String(), ",]")))
	}
	return fmt.Sprintf("{%s}", strings.TrimRight(buff.String(), ","))
}
