/*
					     (7)                (6)
	(7)	|---[waitingReqs]<-handleRead() <------- |<----(返回结果)|
		|				\					reply|	server端    |
开始	Send()				 \(4)				req  |	conn (5)   |
	(1)	|-->[sendingReqs]->handleWrite()---------|--> (开始处理)-|
                        (2)                (3)
顺时针操作
因为channel request 使用的是指针
所以不怕有了reply放进request之后得不到对应结果
*****问题就是server端控制一个client的请求结果有序返回*****
 */



package client

import (
	"bufio"
	"context"
	"errors"
	"github.com/HDT3213/godis/src/interface/redis"
	"github.com/HDT3213/godis/src/lib/logger"
	"github.com/HDT3213/godis/src/lib/sync/wait"
	"github.com/HDT3213/godis/src/redis/reply"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Request struct {
	id uint64
	args [][]byte
	reply redis.Reply
	heartbeat bool
	waiting *wait.Wait
	err error
}

type Client struct {
	addr string
	conn net.Conn
	sendingReqs chan *Request  // waiting sending
	waitingReqs chan *Request  // waiting response
	ticker *time.Ticker   // 干什么?

	ctx context.Context
	cancelFunc context.CancelFunc
	writing *sync.WaitGroup	// 不会立刻被中断
}


const (
	chanSize = 256
	maxWait = 3 * time.Second
)

func MakeClient(addr string) (*Client , error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		addr: addr,
		conn: conn,
		sendingReqs: make(chan *Request),
		waitingReqs: make(chan *Request),

		ctx: ctx,             //close之用
		cancelFunc: cancel,
		writing: &sync.WaitGroup{},
	}, nil
}

func (client *Client)Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	go func() {
		err := client.handleRead()
		logger.Warn(err)
	}()
	go client.heartbeat()
}

func (client *Client)Close() {
	// 先停止新的请求
	close(client.sendingReqs)

	// wait to stop process
	client.writing.Wait()    // 表示还有请求没有完成

	// 可以放心的clean了
	client.cancelFunc() // case <-ctx.Done(): 为true 关掉各个协程
	_ = client.conn.Close()
	close(client.waitingReqs)
}

func (client *Client)handleConnectionError(err error) error {
	err1 := client.conn.Close()
	if err1 != nil {
		if opErr, ok := err1.(*net.OpError); ok {
			if opErr.Err.Error() != "use of clsed network connection" {
				return err1
			}
		} else {
			return err1
		}
	}
	conn, err1 := net.Dial("tcp", client.addr)
	if err1 != nil {
		logger.Error(err1)
		return err1
	}
	client.conn = conn
	go func() {
		_ = client.handleRead()
	}()
	return nil
}

func (client *Client)heartbeat() {
loop:
	for {
		select {
		case <-client.ticker.C:
			client.sendingReqs <- &Request{
				args: [][]byte{[]byte("PING")},
				heartbeat: true,
			}
		case <- client.ctx.Done():   // 每个协程都会如此
			break loop
		}
	}
}

/* 		--------------------
 	第一步是Send()函数，最后也是send函数返回reply
	最开始在想怎么得到结果，这里channel直接使用指针，所有的变化都会反映回原来的
	request，所以直接调request.Reply即可。
*/

func (client *Client) Send(args [][]byte) redis.Reply {
	request := &Request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.sendingReqs <- request
	timeout := request.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return reply.MakeErrReply("server time out")
	}
	if request.err != nil {
		return reply.MakeErrReply("request failed")
	}
	return request.reply
}

/* 		--------------------
		下面两个函数是关于write的
*/

func (client *Client)handleWrite() {
loop:
	for {
		select {
		case req := <- client.sendingReqs: // 取出来
			client.writing.Add(1)	// 确保发送过程不能中断
			client.doRequest(req)   // 发送过去
		case <- client.ctx.Done():
			break loop
		}
	}
}

// handleWrite的最后处理部分
func (client *Client)doRequest(req *Request) {
	bytes := reply.MakeMultiBulkReply(req.args).ToBytes()
	_, err := client.conn.Write(bytes)
	// 重试三次
	i := 0
	for err != nil && i < 3 {
		err = client.handleConnectionError(err)  // 处理错误
		if err == nil {
			_, err = client.conn.Write(bytes)    // 重新发送(若还有错就循环)
		}
		i++
	}
	if err == nil {
		client.waitingReqs <- req // 发送成功就放进等待结果的channel
	} else {
		req.err = err
		req.waiting.Done()			// 表示此请求已完成
		client.writing.Done()       // 表示此请求已完成
	}
}

/* 		--------------------
		下面连个函数是关于read的
*/
func (client *Client) handleRead() error {
	reader := bufio.NewReader(client.conn)
	downloading := false
	expectedArgsCount := 0
	receivedCount := 0
	msgType := byte(0) // first char of msg
	var args [][]byte
	var fixedLen int64 = 0
	var err error
	var msg []byte
	for {
		// read line
		if fixedLen == 0 { // read normal line
			msg, err = reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					logger.Info("connection close")
				} else {
					logger.Warn(err)
				}

				return errors.New("connection closed")
			}
			if len(msg) == 0 || msg[len(msg)-2] != '\r' {
				return errors.New("protocol error")
			}
		} else { // read bulk line (binary safe)
			msg = make([]byte, fixedLen+2)
			_, err = io.ReadFull(reader, msg)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return errors.New("connection closed")
				} else {
					return err
				}
			}
			if len(msg) == 0 ||
				msg[len(msg)-2] != '\r' ||
				msg[len(msg)-1] != '\n' {
				return errors.New("protocol error")
			}
			fixedLen = 0
		}

		// parse line
		if !downloading {
			// receive new response
			if msg[0] == '*' { // multi bulk response
				// bulk multi msg
				expectedLine, err := strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
				if err != nil {
					return errors.New("protocol error: " + err.Error())
				}
				if expectedLine == 0 {
					client.finishRequest(&reply.EmptyMultiBulkReply{})
				} else if expectedLine > 0 {
					msgType = msg[0]
					downloading = true
					expectedArgsCount = int(expectedLine)
					receivedCount = 0
					args = make([][]byte, expectedLine)
				} else {
					return errors.New("protocol error")
				}
			} else if msg[0] == '$' { // bulk response
				fixedLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
				if err != nil {
					return err
				}
				if fixedLen == -1 { // null bulk
					client.finishRequest(&reply.NullBulkReply{})
					fixedLen = 0
				} else if fixedLen > 0 {
					msgType = msg[0]
					downloading = true
					expectedArgsCount = 1
					receivedCount = 0
					args = make([][]byte, 1)
				} else {
					return errors.New("protocol error")
				}
			} else { // single line response
				str := strings.TrimSuffix(string(msg), "\n")
				str = strings.TrimSuffix(str, "\r")
				var result redis.Reply
				switch msg[0] {
				case '+':
					result = reply.MakeStatusReply(str[1:])
				case '-':
					result = reply.MakeErrReply(str[1:])
				case ':':
					val, err := strconv.ParseInt(str[1:], 10, 64)
					if err != nil {
						return errors.New("protocol error")
					}
					result = reply.MakeIntReply(val)
				}
				client.finishRequest(result)
			}
		} else {
			// receive following part of a request
			line := msg[0 : len(msg)-2]
			if line[0] == '$' {
				fixedLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
				if err != nil {
					return err
				}
				if fixedLen <= 0 { // null bulk in multi bulks
					args[receivedCount] = []byte{}
					receivedCount++
					fixedLen = 0
				}
			} else {
				args[receivedCount] = line
				receivedCount++
			}

			// if sending finished
			if receivedCount == expectedArgsCount {
				downloading = false // finish downloading progress

				if msgType == '*' {
					reply := reply.MakeMultiBulkReply(args)
					client.finishRequest(reply)
				} else if msgType == '$' {
					reply := reply.MakeBulkReply(args[0])
					client.finishRequest(reply)
				}


				// finish reply
				expectedArgsCount = 0
				receivedCount = 0
				args = nil
				msgType = byte(0)
			}
		}
	}
}

// handleRead的最后处理过程
// 拿到结果后对用请求并Done()两次
/*
这里有个问题青后的请求和结果能对应上吗
	就是:两个请求都发送过去了，但是后边一个先得到结果
这要看server端的client是不是有序的，因为client的逻辑是不保证的
就是conn.Read()完完整指令后执行是不是阻塞的
*/
func (client *Client)finishRequest(reply redis.Reply) {
	request := <-client.waitingReqs
	request.reply = reply // 对应结果
	if request.waiting != nil {
		request.waiting.Done()			// 表明此请求完成
	}
	client.writing.Done()
}