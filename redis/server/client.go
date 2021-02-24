package server

import (
	"github.com/HDT3213/godis/src/lib/sync/atomic"
	"github.com/HDT3213/godis/src/lib/sync/wait"
	"net"
	"sync"
	"time"
)

type Client struct {
	conn net.Conn

	// 带有timeout的waitgroup 优雅的关闭
	// 其实就是在一定时间内没有Done()也就是不能Wait()成功就直接结束掉
	// 和外部对接的
	WaitingReply wait.Wait
	// lock while server sending response
	// 不明白为什么要有这个，执行不是阻塞式的吗
	mu sync.Mutex

	/*
		这里应该了解到所有需要标记的变量或者记号都应该
		是atomic原子操作的, 用锁也可以
	 */
	// 判断是否在接收指令 (为什么不用atomic.Value)
	// 指的是在接收指令中间还是指令开头，要和WaitingReply区别开
	isRecving atomic.AtomicBool
	/* 下面是redis RESP协议的参数相关信息 */
	/*  array第一行指定的参数个数 */
	expectedArgsCount uint32
	/* 已接收的参数个数 */
	recivedCount uint32
	/* 已接收的参数 */
	args [][]byte


	// 应该还有一个参数就是client 阻塞的keys数组,
	//一但某个key被push时查到了这个client,
	//那就把剩余的key里的这个client全部remove掉(这里就需要通过
	//一个key-->找到client-->找到剩余的key)
	blockKeys []string
	// 订阅信息
	subs map[string]bool
}

func MakeClient(conn net.Conn) *Client {
	return &Client{
		conn: conn,
	}
}

func (c *Client)Close() error {
	// 数据发送时还用wait.Add(1) 发送完成后wait.Done()
	// 所以最多再等10s，不主动退出就强制关闭
	c.WaitingReply.WaitWithTimeout(10 * time.Second)	// 一但要关闭只能等待一定时间
	c.conn.Close()
	return nil
}


func (c *Client)Write(b []byte) error {
	if b == nil || len(b) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.conn.Write(b)
	return err
}

func (c *Client)SubsChannel(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		return
	}
	c.subs[channel] = true
}

func (c *Client)UnSubsChannel(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		return
	}
	delete(c.subs, channel)
}

func (c *Client)SubsCount() int {
	if c.subs == nil {
		return 0
	}
	return len(c.subs)
}

func (c *Client)GetChannels() []string {
	if c.subs == nil {
		return nil
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel,_ := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}