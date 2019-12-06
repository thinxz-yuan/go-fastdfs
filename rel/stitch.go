package rel

// 针脚定义
// ---------- ----------
type Stitch struct {
	// 针脚序号
	no int
	// 相联的器件
	Relation []*Relation
}

func (s *Stitch) AddRelation(r *Relation) {
	s.Relation = append(s.Relation, r)
}

//// 创建针脚对象
//// ---------- ----------
//func NewStitch(no int) *Stitch {
//	return &Stitch{
//		no:       no,
//		Relation: []Relation{},
//	}
//}

// 针脚号
// ---------- ----------
func (s *Stitch) GetNo() int {
	return s.no
}
