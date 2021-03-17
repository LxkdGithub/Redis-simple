package sortedset

import "math/rand"

// 跳表实现 链接[https://www.cnblogs.com/Finley/p/12854599.html]

const (
	maxLevel = 16
)

type Element struct {
	Member string
	Score float64
}
type Node struct {
	Element
	backward *Node
	level []*Level // Level[0] is base level
}

type Level struct {
	forward *Node
	span int64 		// 该元素在该层跳过多少元素到下一元素
}

type skiplist struct {
	header *Node
	tail *Node
	length int64
	level int16
}

func makeNode(level int16, score float64, member string) *Node {
	n := &Node{
		// 原来匿名属性隐藏有同名而已
		Element: Element{
			Score: score,
			Member: member,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func makeSkiplist() *skiplist {
	return &skiplist{
		header: makeNode(maxLevel, 0, ""),
		level: 1,
	}
}

// generate a level number which between [1, and maxLevel]
func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31() & 0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}
	if level < maxLevel {
		return level
	}
	return maxLevel
}

/*
 **********************  重点 特别是rank[]的理解(作用是确定node的span)
 */
func (skiplist *skiplist)insert(member string, score float64)*Node {
	update := make([]*Node, maxLevel) // link new node with node in `update`
	rank := make([]int64, maxLevel)

	// find position to insert
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		if i == skiplist.level - 1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i + 1] // store rank that is crossed to reach the insert position
		}
		if node.level[i] != nil {
			// traverse the skip list
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score ||
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) { // same score, different key
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	level := randomLevel()
	// extend skiplist level
	if level > skiplist.level {
		for i := skiplist.level; i < level; i++ {
			rank[i] = 0
			update[i] = skiplist.header
			update[i].level[i].span = skiplist.length
		}
		skiplist.level = level
	}

	// make node and link into skiplist
	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node

		// update span covered by update[i] as node is inserted here
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// increment span for untouched levels
	for i := level; i < skiplist.level; i++ {
		update[i].level[i].span++
	}

	// set backward node
	if update[0] == skiplist.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		skiplist.tail = node
	}
	skiplist.length++
	return node
}

func (skiplist *skiplist)removeNode(node *Node, update []*Node) {
	for i := int16(0); i < skiplist.level; i++ {
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			// TODO 不明白
			update[i].level[i].span--
		}
	}
	// 为什么只更新level[0] 是因为backward是在node里的，而不是level里的
	if node.level[0].forward != nil {     // node不是tail
		node.level[0].forward.backward = node.backward
	} else {							  // node是tail
		skiplist.tail = node.backward
	}
	// update level
	for skiplist.level > 1 && skiplist.header.level[skiplist.level-1].forward == nil {
		skiplist.level--
	}
	skiplist.length--  // 删除node的原因
}

func (skiplist *skiplist)remove(member string, score float64) bool {
	// 分为两步 (1).找到节点 (2).删除节点
	//1. find this node  这里应该封装为函数
	update := make([]*Node, maxLevel)
	n := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for n.level[i].forward != nil &&
			(n.level[i].forward.Score < score ||
				(n.level[i].forward.Score == score && n.level[i].forward.Member < member)) {
			n = n.level[i].forward
		}
		update[i] = n
	}
	// 得到待删节点
	n = n.level[0].forward
	// 2.删除
	if n != nil && score == n.Score && n.Member == member {
		skiplist.removeNode(n, update)
		return true
	}
	return false
}

// 用于   Zrank()  方法
func (skiplist *skiplist)getRank(member string, score float64) int64 {
	var rank int64 = 0
	n := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for n.level[i].forward != nil &&
			(n.level[i].forward.Score < score ||
				(n.level[i].forward.Score == score && n.level[i].forward.Member <= member)) {
			rank += n.level[i].span
			n = n.level[i].forward
		}

		if n.Member == member {
			return rank
		}
	}
	return 0
}

/*
 ******************* 重点 已理解
 */
// 按照排名返回节点
func (skiplist *skiplist)getByRank(rank int64) *Node {
	var i int64 = 0		// 累计之用 以便检测是否到达
	n := skiplist.header

	// 从最上层开始 底层是0
	for level := skiplist.level - 1; level >= 0; level-- {
		// 循环向前，知道下一个过了排名 然后向下一个level
		for n.level[level].forward != nil && (i + n.level[level].span <= rank) {
			i += n.level[level].span
			n = n.level[level].forward
		}
		if i == rank {
			return n
		}
	}
	return nil
}

func (skiplist *skiplist)HasInRange(min *ScoreBorder, max *ScoreBorder) bool {
	// First pass the condition of
	// [10, 1] or (3, 3]
	if min.Value > max.Value || (min.Value == max.Value && (max.Exclude || min.Exclude)) {
		return false
	}
	// min > tail
	n := skiplist.tail
	if n == nil || !min.less(n.Score) {
		return false
	}
	// max < header
	// header不存储 Node
	n = skiplist.header.level[0].forward
	if n == nil || !max.greater(n.Score) {
		return false
	}
	return true
}

/*
 ********** 重点
 */
func (skiplist *skiplist)getFirstInScoreRange(min *ScoreBorder, max *ScoreBorder) *Node {
	if !skiplist.HasInRange(min, max) {
		return nil
	}
	n := skiplist.header
	for level := skiplist.level - 1; level >= 0;level-- {
		for n.level[level].forward != nil && !min.less(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}

	n = n.level[0].forward
	if !max.greater(n.Score) {
		return nil
	}
	return n
}

// 注意和上一个函数的区别
func (skiplist *skiplist)getLastInScoreRange(min *ScoreBorder, max *ScoreBorder) *Node {
	if !skiplist.HasInRange(min, max) {
		return nil
	}
	n := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && max.greater(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}
	if !min.less(n.Score) {
		return nil
	}
	return n
}

func (skiplist *skiplist)RemoveRangeByScore(min *ScoreBorder, max *ScoreBorder) (removed []*Element) {
	update := make([]*Node, maxLevel)
	removed = make([]*Element, maxLevel)

	// find backward nodes of each level
	n := skiplist.header
	for i := skiplist.level - 1; i >=0 ;i-- {
		for n.level[i].forward != nil && min.less(n.level[i].forward.Score) {
			n = n.level[i].forward
		}
		update[i] = n
	}

	// get the first to remove
	n = n.level[0].forward
	for n != nil {
		if !max.greater(n.Score) {	// nothing to remove
			return
		}
		next := n.level[0].forward
		removeElement := n.Element
		removed = append(removed, &removeElement) // 用于返回用的(指针不是什么问题)
		skiplist.removeNode(n, update)
		n = next
	}
	return
}


func (skiplist *skiplist)RemoveRangeByRank(start int64, stop int64) (removed []*Element) {
	var i int64 = 0
	update := make([]*Node, maxLevel)
	removed = make([]*Element, maxLevel)

	n := skiplist.header
	// scan from top level
	node := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}

	i++		// start position
	n = node.level[0].forward

	for n != nil && i < stop {
		next := n.level[0].forward
		removeElement := n.Element
		removed = append(removed, &removeElement)
		skiplist.removeNode(node, update)
		n = next
		i++
	}
	return
}