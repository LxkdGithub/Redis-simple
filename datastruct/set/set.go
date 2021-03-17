package set

import "redis.simple/datastruct/dict"

// redis内部是使用inset和hashtable两种方式存储的
// 数值类型并且元素数量不超过512时使用inset存储
// 否则使用hashtable
// 这里直接使用hashtable也就是Dict类型
type Set struct {
	dict dict.Dict
}

func Make() *Set {
	return &Set{
		dict: dict.MakeConcurrent(8),
	}
}

func MakeFromVals(members ...string) *Set {
	set := &Set{
		dict: dict.MakeConcurrent(len(members)),
	}
	for _, member := range members {
		set.Add(member)
	}

	return set
}

//用于   Set.SAdd()   方法
func (set *Set)Add(val string) int {
	return set.dict.Put(val, true, dict.STRING)
}

//用于   Set.SRem()   方法
func (set *Set)Remove(val string) int {
	return set.dict.Remove(val)
}

//用于   Set.SIsMember()   方法
func (set *Set)Has(val string) bool {
	_, exists := set.dict.Get(val)
	return exists
}

//用于   Set.Scard()   方法
func (set *Set)Len() int {
	return set.dict.Len()
}

// 用于  Set.SMembers() 方法
func (set *Set)ToSlice() []string {
	slice := make([]string, set.Len())
	i := 0
	set.dict.ForEach(func(key string, val interface{}) bool {
		if i > len(slice) {
			slice[i] = key
		} else {
			slice = append(slice, key)
		}
		i++
		return true
	})
	return slice
}


func (set *Set)ForEach(consumer func(member string) bool ) {
	set.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

//用于   Set.SInter() & Set.InterStore()   方法
func (set *Set)Intersect(author *Set) *Set {
	if set == nil {
		panic("set is nil")
	}
	result := Make()
	author.ForEach(func(member string) bool {
		if set.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

//用于   Set.Union() & Set.UnionStore   方法
func (set *Set)Union(author *Set) *Set {
	if set == nil {
		panic("set is nil")
	}

	result := Make()
	author.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	set.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	return result
}

//用于   Set.Diff() & Set.DiffStore()   方法
func (set *Set)Diff(author *Set) *Set {
	if set == nil {
		panic("set is nil")
	}

	result := Make()
	set.ForEach(func(member string) bool {
		if !author.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

// 不知作用
func (set *Set)RandomMembers(limit int) [] string {
	return set.dict.RandomKeys(limit)
}

// 用于    Set.RandomMember()   方法
func (set *Set)RandomDistinctMembers(limit int) []string {
	return set.dict.RandomDistinctKeys(limit)
}

func (set *Set)Move(dst *Set, val string) int {
	// 不存在的错误交给DB层
	if set == nil {
		panic("set is nil")
	}

	if set.Has(val) {
		r := set.Remove(val)
		dst.Add(val)
		if r == 1 {
			return 1
		}
	}
	// 说明不存在 只要source存在并成功删除就可以返回1
	return 0
}