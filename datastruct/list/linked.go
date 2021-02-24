package list

import (
	"bytes"
)

type LinkedList struct {
	first *node
	last *node
	size int
}

type node struct {
	val interface{}
	prev *node
	next *node
}

// 没有的话创建工作和pop后的删除操作都是在外边完成的
// 所以这里放心操作 不会出现list.first = list.last = nil的情况

/*
很多操作要靠DB层面的
 */

// 返回op后list总数
func (list *LinkedList)lpush(val interface{}) int {
	// 有没有在DB层面就知道了
	// 这里只管底层数据结构的操作
	if list == nil {
		panic("list is nil")
	}
	newNode := &node{
		val: val,
	}
	if list.last == nil {
		// empty
		list.first = newNode
		list.last = newNode
	} else {
		newNode.next = list.first
		list.first.prev = newNode
		list.first = newNode
	}
	list.size++
	return list.size
}

// 返回op后list总数
func (list *LinkedList)rpush(val interface{}) int {
	if list == nil {
		panic("list is nil")
	}
	newNode := &node{
		val: val,
	}
	if list.last == nil {
		// empty
		list.first = newNode
		list.last = newNode
	} else {
		newNode.prev = list.last
		list.last.next = newNode
		list.last = newNode
	}
	list.size++
	return list.size
}

// 返回pop的element
func (list *LinkedList)lpop() interface{} {
	if list == nil {
		panic("list is nil")
	}
	var val interface{}
	if list.first != nil {
		val = list.first.val
		if list.last == list.first {
			list.first, list.last = nil, nil
		} else {
			list.first = list.first.next
		}
	} else {
		val = nil
	}
	return val
}

// 返回pop的element
func (list *LinkedList)rpop() interface{} {
	if list == nil {
		panic("list is nil")
	}
	var val interface{}
	if list.last != nil {
		val = list.first.val
		if list.last == list.first {
			list.first, list.last = nil, nil
		} else {
			list.last = list.last.prev
		}
	} else {
		val = nil
	}
	return val
}

// return key and element poped
// 这里只返回pop的element
// 这里的的逻辑应该从DB层面写, 这里只做最底层的操作
// 无逻辑限制
//func (list *LinkedList)blpop(timeout int, client redis.Client) interface{} {
//	//c := time.NewTicker(time.Duration(timeout))
//	//select {
//	//case <-c.C:
//	//	return nil
//	//case client.block
//	//}
//	return nil
//}

func (list *LinkedList)brpop() interface{} {
	return nil
}

// 这里要注意验证位置的合理(在DB层面再写验证逻辑，然后调用这里)
func (list *LinkedList)lrange(start int, stop int) []interface{} {
	if list.size == 0 {
		return nil
	}
	res := make([]interface{}, list.size)
	i := 0
	n := list.first
	for n != nil {
		res[i] = n.val
		i++
		n = n.next
	}
	return res
}

// 返回count
func (list *LinkedList)llen() int {
	return list.size
}

// 返回移除个数
// 大的类型比如list,set这些是在DB层面验证，例如list内部的类型比较在函数内完成
// 数据都直接按照[]byte存储就好
func (list *LinkedList)lrem(count int, val interface{}) int {
	if list.size == 0 {
		return 0
	}
	relCount := 0
	n := list.first
	for n != nil {
		if bytes.Compare(n.val.([]byte), val.([]byte)) == 0 {
			n.prev.next = n.next
			n.next.prev = n.prev
			relCount++
		}
		n = n.next  // next就好
	}
	return relCount
}

// 没有返回nil
func (list *LinkedList)lindex(index int) interface{} {
	if index < list.size / 2 {
		n := list.first
		for i := 0;i < index;i++ {
			n = n.next
		}
		return n.val
	} else {
		n := list.last
		for i:=list.size-1;i>index;i-- {
			n = n.prev
		}
		return n.val
	}
}

// 返回是否成功
func (list *LinkedList)ltrim(start, stop int) bool {
	// assert 0 <= start < end < list.size
	// DB保证的, 如果不是，那list就变为空
	i := 0
	n := list.first
	for i < start {
		n = n.next
	}
	list.first = n
	n.prev = nil
	for i := start; i< stop; i++ {
		n = n.next
	}
	list.last = n
	n.next = nil
	return true
}

// 是否成功
func (list *LinkedList)lset(index int, val interface{}) bool {
	// DB已保证index不超过范围(0, list.size-1)
	n := list.first
	for i := 0; i < index; i++ {
		n = n.next
	}
	n.val = val
	return true
}

// 返回个数
func (list *LinkedList)linsert(isBefore bool, pivot interface{} , val interface{}) int {
	n := list.first
	for n != nil {
		if n.val == pivot {
			if isBefore {
				newNode := &node{
					val: val,
					prev: n.prev,
					next: n,
				}
				n.prev = newNode
			} else {

			}
		}
	}
	return 0 // count
}



