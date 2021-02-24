package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

type HashFunc func([]byte) uint32

type Map struct {
	hashFunc HashFunc
	replicas int		// 类似节点虚拟倍数
	keys []int			// 虚拟节点的环
	hashMap map[int]string // 虚拟节点和真实节点的对应关系
}


func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		replicas: replicas,
		hashFunc: fn,
		hashMap:  make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map)IsEmpty() bool {
	return len(m.hashMap) == 0
}

func (m *Map)Add(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		for i := 0;i < m.replicas; i++ {
			hash := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash) //添加虚拟节点
			m.hashMap[hash] = key // 添加对应关系
		}
	}
	sort.Ints(m.keys) // 使环有序
}

// hash tag
func getPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 { // 表示没有
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg + 1 || beg > end {
		return key
	}
	return key[beg+1:end]
}


// calculate the closest item in the hash circle
// 就是为每一个key对应的真实节点
func (m *Map)Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	partitionKey := getPartitionKey(key)
	hash := int(m.hashFunc([]byte(partitionKey)))

	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]] // 获得真实节点
}