package rel

import "fmt"

// 电阻
// ----------
type R struct {
	Component
}

// 电阻 - 计算
// ----------
func (r *R) calculate() {
	fmt.Println(fmt.Sprintf("%s calculating ...", r.name))
	fmt.Println()
	// 计算根据器件属性值, 及计算规则, 计算针脚数据

	// 计算完毕信息传递
	r.Component.calculate()

	fmt.Println()
	fmt.Println(fmt.Sprintf("%s finish ", r.name))
}
