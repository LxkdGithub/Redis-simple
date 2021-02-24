package set

// redis内部是使用inset和hashtable两种方式存储的
// 数值类型并且元素数量不超过512时使用inset存储
// 否则使用hashtable
type set struct {

}
