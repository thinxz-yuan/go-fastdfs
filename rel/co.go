package rel

//
//// 针脚定义
//// ----------
//type Co struct {
//	// 针脚数组, 定义
//	Stitch map[int]Stitch
//}
//
//func (co *Co) Init() {
//	co.Stitch = make(map[int]Stitch)
//}
//
//// 添加针脚 {没有添加的针脚号为空针脚}
//// ----------
//func (co *Co) AddStitch(no int, target Component, targetNo int) {
//	s, ok := co.Stitch[no]
//	if !ok {
//		// 初始化针脚
//		co.Stitch[no] = Stitch{
//			no:       no,
//			Relation: [] *Relation{},
//		}
//	}
//
//	// 添加针脚关系
//	s.Relation = append(s.Relation, &Relation{
//		No:        targetNo,
//		Component: target,
//	})
//}
//
//// 计算
//// ----------
//func (co *Co) Calculate() {
//
//}
//
//// 触发事件
//// ----------
//func (co *Co) Event(event string) {
//
//}
