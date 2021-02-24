/*
	用于多个key同时锁住，已达到类似于MSETNX的目的
 */


package lock

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type Locks struct {
	table []*sync.RWMutex
}

const (
	prime32 = uint32(16777619)
)

// 初始化锁, 一次初始化多个
func Make(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)
	for i:=0;i<tableSize;i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{
		table: table,
	}
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}


// 好家伙，hash方法和dict存取一样，但是hash槽大小不同，
// 有得对应一次
func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & uint32(hashCode)
}

func (locks *Locks)Lock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Lock()
}

func (locks *Locks)RLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RLock()
}

func (locks *Locks)UnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Unlock()
}

func (locks *Locks)RUnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RUnlock()
}


// 锁之前先获得需要锁住那些位置
func (locks *Locks)toLockIndices(keys []string, reserve bool) []uint32 {
	indexMap := make(map[uint32]bool)
	for _, k := range keys {
		index := locks.spread(fnv32(k))
		indexMap[index] = true
	}
	// 为什么不直接放进indices,因为index会有重复的
	// 思来想去，只能先放进map了
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	// 下面是排序 注意inverse
	sort.Slice(indices, func(i, j int) bool {
		if !reserve {
			return indices[i] < indices[j]
		} else {
			return indices[i] > indices[j]
		}
	})
	return indices
}

// 开始锁
func (locks *Locks)Locks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		locks.table[index].Lock()
	}
}

func (locks *Locks)RLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		locks.table[index].RLock()
	}
}

func (locks *Locks)UnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		locks.table[index].Unlock()
	}
}

func (locks *Locks)RUnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		locks.table[index].RUnlock()
	}
}

func GoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	stk := strings.TrimPrefix(string(buf[:n]), "goroutine ")
	idField := strings.Fields(stk)[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}


func debug(testing.T) {
	lm := Locks{}
	size := 10
	var wg sync.WaitGroup
	wg.Add(size)
	for i := 0;i < size;i++ {
		go func(i int) {
			lm.Locks("1", "2")
			println("go: " + strconv.Itoa(GoID()))
			time.Sleep(time.Second)
			println("go: " + strconv.Itoa(GoID()))
			lm.UnLocks("1", "2")
		}(i)
	}
	wg.Wait()
}


