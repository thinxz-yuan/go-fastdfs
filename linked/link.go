package linked

type Object interface{}

type Node struct {
	data Object
	next *Node
}

type List struct {
	size int   // 车辆数量
	head *Node // 车头
	tail *Node // 车尾
}

func (list *List) Init() {
	(*list).size = 0   // 此时链表是空的
	(*list).head = nil // 没有车头
	(*list).tail = nil // 没有车尾
}

func (list *List) Put(data Object) bool {
	return list.append(
		&Node{
			data: data,
			next: nil,
		})
}

// 获取第一个元素, 并从链表中删除
// ---------- ----------
func (list *List) Push() Object {
	n := list.get(0)
	if n == nil {
		return nil
	} else {
		//
		list.remove(0, n)
		return n.data
	}
}

func (list *List) append(node *Node) bool {
	if node == nil {
		return false
	}

	(*node).next = nil
	// 将新元素放入单链表中
	if (*list).size == 0 {
		(*list).head = node
	} else {
		oldTail := (*list).tail
		(*oldTail).next = node
	}

	// 调整尾部位置，及链表元素数量
	(*list).tail = node // node成为新的尾部
	(*list).size++      // 元素数量增加
	return true
}

// 获取某个位置的元素
// ---------- ----------
func (list *List) get(i int) *Node {
	if i >= (*list).size {
		return nil
	}
	item := (*list).head
	for j := 0; j < i; j++ { // 从head数i个
		item = (*item).next
	}
	return item
}

// 删除元素
// ---------- ----------
func (list *List) remove(i int, node *Node) bool {
	if i >= (*list).size {
		return false
	}

	if i == 0 { // 删除头部
		node = (*list).head
		(*list).head = (*node).next
		if (*list).size == 1 { // 如果只有一个元素，那尾部也要调整
			(*list).tail = nil
		}
	} else {
		preItem := (*list).head
		for j := 1; j < i; j++ {
			preItem = (*preItem).next
		}

		node = (*preItem).next
		(*preItem).next = (*node).next

		if i == ((*list).size - 1) { // 若删除的尾部，尾部指针需要调整
			(*list).tail = preItem
		}
	}
	(*list).size--
	return true
}

//
//func (list *List) Append(node *Node) {
//	(*list).head = node // 这是单链表的第一个元素，也是链表的头部
//	(*list).tail = node // 同时是单链表的尾部
//	(*list).size = 1    // 单链表有了第一个元素
//}
//
//func (list *List) Append(node *Node) {
//	if (*list).size == 0 { // 无元素的时候添加
//		(*list).head = node // 这是单链表的第一个元素，也是链表的头部
//		(*list).tail = node // 同时是单链表的尾部
//		(*list).size = 1    // 单链表有了第一个元素
//	} else { // 有元素了再添加
//		oldTail := (*list).tail
//		(*oldTail).next = node  // node放到尾部元素后面
//		(*list).tail = node     // node成为新的尾部
//		(*list).size++          // 元素数量增加
//	}
//}

//
//func (list *List) Insert(node *Node) bool {
//	if node == nil {
//		return false
//	}
//
//	(*node).next = (*list).head   // 领导小舅子排到之前第一名前面
//	(*list).head = node           // 领导小舅子成为第一名
//	(*list).size++
//
//	return true
//}

// 插入元素
// ---------- ----------
//func (list *List) insert(i int, node *Node) bool {
//	// 空的节点、索引超出范围和空链表都无法做插入操作
//	if node == nil || i > (*list).size || (*list).size == 0 {
//		return false
//	}
//	if i == 0 { // 直接排第一，也就领导小舅子才可以
//		(*node).next = (*list).head
//		(*list).head = node
//	} else {
//		// 找到前一个元素
//		preItem := (*list).head
//		for j := 1; j < i; j++ { // 数前面i个元素
//			preItem = (*preItem).next
//		}
//		// 原来元素放到新元素后面,新元素放到前一个元素后面
//		(*node).next = (*preItem).next
//		(*preItem).next = preItem
//	}
//	(*list).size++
//	return true
//}
