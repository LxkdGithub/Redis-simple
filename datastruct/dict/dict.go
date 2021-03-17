package dict

type Consumer func(key string, val interface{})bool

// 这里的Dict是整个的Dict也就是最大的存储结构
// 所以只有对key的操作(当然dict类型的操作也是可以的)
// 取到Value后使用什么方法是类型具体的
type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}, types uint8) (result int)
	PutIfAbsent(key string, val interface{}, types uint8) (result int)
	PutIfExists(key string, val interface{}, types uint8) (result int)
	Remove(key string) (result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
}


// Entity 是取key之后得到的结构
// 包含类型 和 该类型的结构比如 List Set等
// Dict里边的Value
type Entity interface {
	Type() uint8	// 返回类型(如下所示)
}

const (
	STRING = 0
	LIST = 1
	HASH = 2
	SET = 3
	ZSET = 4
	BITMAP = 5
)
