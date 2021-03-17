package list

import (
	"bytes"
	"reflect"
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


func Make() *LinkedList {
	return &LinkedList{
		first: nil,
		last: nil,
		size: 0,
	}
}


// 返回op后list总数
func (list *LinkedList)Lpush(val interface{}) int {
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
func (list *LinkedList)Rpush(val interface{}) int {
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
func (list *LinkedList)Lpop() interface{} {
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
func (list *LinkedList)Rpop() interface{} {
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

func (list *LinkedList)Brpop() interface{} {
	return nil
}

// 这里要注意验证位置的合理(在DB层面再写验证逻辑，然后调用这里)
func (list *LinkedList)Lrange(start int, stop int) []interface{} {
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
func (list *LinkedList)Llen() int {
	return list.size
}

// 返回移除个数
// 大的类型比如list,set这些是在DB层面验证，例如list内部的类型比较在函数内完成
// 数据都直接按照[]byte存储就好
func (list *LinkedList)Lrem(count int, val interface{}) int {
	if list.size == 0 {
		return 0
	}
	// 值相等的全部删除(将count设置的大一些)
	if count == -1 {
		count = list.size
	}
	relCount := 0
	n := list.first
	for n != nil {
		if relCount >= count {
			return relCount
		}
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
func (list *LinkedList)Lindex(index int) interface{} {
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
func (list *LinkedList)Ltrim(start, stop int) bool {
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
func (list *LinkedList)Lset(index int, val interface{}) bool {
	// DB已保证index不超过范围(0, list.size-1)
	n := list.first
	for i := 0; i < index; i++ {
		n = n.next
	}
	n.val = val
	return true
}

// 返回个数
func (list *LinkedList)Linsert(isBefore bool, pivot interface{} , val interface{}) int {
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


func (list *LinkedList)Contains(val interface{}) bool {
	node := list.first
	obj_types := reflect.TypeOf(val)
	obj_value := reflect.ValueOf(val)
	for node != nil {
		if reflect.TypeOf(node.val) == obj_types &&
			reflect.ValueOf(node.val) == obj_value{
			return true
		}
		node = node.next
	}
	return false
}

func (list *LinkedList)Foreach(F func(i int, c interface{}) bool) {
	j := 0
	node := list.first
	for node != nil {
		F(j, node.val)
		node = node.next
		j++
	}
}




