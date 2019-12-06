package event

import "../linked"

type Event struct {
	Data linked.Object
	//
	//// 事件链表
	//events linked.List
}

//func (e Event) Init() {
//	e.events.Init()
//}
//
//func (e Event) Add(data *linked.Object) {
//	e.events.Put(data)
//}
//
//func (e Event) Get() *linked.Object {
//	return e.events.Push()
//}
