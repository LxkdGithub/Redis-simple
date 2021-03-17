package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)



// redis 取到key后判断类别之用
type Value struct {
	types uint8 // 类型(从0-5)
	val interface{}
}

func (value *Value)Type() uint8 {
	return value.types
}

type Shard struct {
	m map[string]Value
	mutex sync.RWMutex
}

type ConcurrentDict struct {
	table []*Shard
	count int32
}


// 不明白 Shard的个数要适当计算不能随意制指定
// 构造Shard时初始化
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	} else {
		return int(n + 1)
	}
}


func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*Shard, shardCount)
	for i:=0;i<shardCount;i++ {
		table[i] = &Shard{
			m: make(map[string]Value),
		}
	}
	d := &ConcurrentDict{
		table: table,
		count: 0,
	}
	return d
}

const (
	prime32 = uint32(16777619)
)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i:=0;i<len(key);i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 定位shard的index, 第一次hashcode的对应
// 其实hash % n 就是 hash ^ (n - 1)
func (dict *ConcurrentDict)spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	//tableSize 是2的幂次，
	//所以hashCode^(tableSize-1)即可得到index
	tableSize := len(dict.table)
	return uint32(tableSize - 1) ^ uint32(hashCode)
}

// 根据上一个函数的index取Shard
func (dict *ConcurrentDict)getShard(index uint32) *Shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}


// 接下来就是Get和Put方法了
func (dict *ConcurrentDict)Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	// 锁住
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()
	val, exists = shard.m[key]
	return
}


func (dict *ConcurrentDict)Put(key string, val interface{}, types uint8) (resultCode int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	// 锁住
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	value := Value{
		types: types,
		val: val,
	}
	// 还要加count,所以要判断是否存在
	if _, ok := shard.m[key]; ok {
		shard.m[key] = value
		return
	} else {
		shard.m[key] = value
		dict.addCount(1)
		return 1
	}
}


func (dict *ConcurrentDict)Len() (int) {
	if dict == nil {
		panic("dict is nil")
	}
	// 这里注意&取地址符是加在了dict.count整体上
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict)addCount(count int) {
	if dict == nil {
		panic("dict is nil")
	}
	atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict)PutIfExists(key string, val interface{}, types uint8) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	value := Value{
		types: types,
		val: val,
	}
	if _, ok := shard.m[key]; ok {
		shard.m[key] = value
		return 1
	} else {
		return 0
	}
}

func (dict *ConcurrentDict)PutIfAbsent(key string, val interface{}, types uint8) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	value := Value{
		types: types,
		val: val,
	}
	if _, ok := shard.m[key]; ok {
		return 0
	} else {
		shard.m[key] = value
		dict.addCount(1)
		return 1
	}
}


func (dict *ConcurrentDict)Remove(key string) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		delete(shard.m, key)
		return 1
	} else {
		return 0
	}
}


// but may not conduct on which inserted during the traversal(traverse 遍历)
func (dict *ConcurrentDict)ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, shard := range dict.table {
		for key, val := range shard.m {
			shard.mutex.RLock()
			ifContinue := consumer(key, val)
			defer shard.mutex.RUnlock()
			if !ifContinue{
				return
			}
		}
	}
}


// 为了不让len失效，超过len时要append
func (dict *ConcurrentDict)Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.Foreach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}


func (shard *Shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return key
	}
	return ""
}

func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)

	result := make([]string, limit)
	for i := 0; i < limit; {
		shard := dict.getShard(uint32(rand.Intn(shardCount)))
		if shard == nil {
			continue
		}
		key := shard.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	shardCount := len(dict.table)
	result := make(map[string]bool)
	for len(result) < limit {
		shardIndex := uint32(rand.Intn(shardCount))
		shard := dict.getShard(shardIndex)
		if shard == nil {
			continue
		}
		key := shard.RandomKey()
		if key != "" {
			result[key] = true
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}
