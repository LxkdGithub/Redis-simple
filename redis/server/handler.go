package server

import (
	"bufio"
	"context"
	"github.com/HDT3213/godis/src/cluster"
	"github.com/HDT3213/godis/src/config"
	DBImpl "github.com/HDT3213/godis/src/db"
	"github.com/HDT3213/godis/src/interface/db"
	"github.com/HDT3213/godis/src/lib/logger"
	"github.com/HDT3213/godis/src/lib/sync/atomic"
	"github.com/HDT3213/godis/src/redis/reply"
	"io"
	"net"
	"strconv"
	"sync"
)

var (
	UnknownErrReplyBytes = []byte("-ERR unknow\r\n")
)


type Handler struct {
	/*
	 * 记录活跃的客户端链接
	 * 类型为 *Client -> placeholder
	 */
	activeConn sync.Map
	/* 关闭标志位 listener出错判断标识 */
	closing atomic.AtomicBool

	/* 数据库引擎 */
	db db.DB
}

func MakeHandler() *Handler {
	var db db.DB
	if config.Properties.Peers != nil &&
		len(config.Properties.Peers) > 0 {
		db = cluster.MakeCluster()
	} else {
		db = DBImpl.MakeDB()
	}
	return &Handler{
		db: db,
	}
}


func (h *Handler) closeClient(client *Client) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

func (h *Handler)Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() == true {
		_ = conn.Close()
	}

	client := &Client{
		conn: conn,
	}
	// 将client存储到sync.Map中去
	h.activeConn.Store(client, 1)

	reader := bufio.NewReader(conn)
	var fixedLen int64 = 0 // 将读取的长度
	var err error
	var msg []byte
	for {
		// 第一步. 接受数据
		if fixedLen == 0 {
			msg, err = reader.ReadBytes('\n')
			if len(msg) == 0 || msg[len(msg)-2] != '\r' {
				errReply := &reply.ProtocolErrReply{Msg: "invalid multibulk length"}
				_, _ = client.conn.Write(errReply.ToBytes())
			}
		} else { // 读取到第二行时，根据给出的长度读取 比如*3$3SET\r\n
			msg = make([]byte, fixedLen + 2)
			_, err = io.ReadFull(reader, msg)
			if len(msg) == 0 || string(msg[len(msg)-2:]) != "\r\n" {
				errReply := &reply.ProtocolErrReply{Msg: "invalid multibulk length"}
				_, _ = client.conn.Write(errReply.ToBytes())
			}
			fixedLen = 0
			// 下边还会存进args去, 在msg也不是个事
		}

		// 第二步. 处理异常
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF { // 后一种是没读够
				logger.Info("connection close")
			} else {
				logger.Warn(err)
			}
			/*
				结束此连接 为什么呢
		 		因为协议错误前边是由内容判断的，已处理过，不是人为，也无需断开连接
				其他的都应该是正常的 因为即使的错误的语法也遵守RESP规范的，所以EOF错误
				就可以判断是连接出了问题
			*/
			_ = client.Close()
			h.activeConn.Delete(client)
			return
		}

		// 第三步. 终于可以解析数据了
		if client.isRecving.Get() == false {
			// isSending==true表示正在就收对方的指令
			// fasle一条新的指令，从头解析 (这是解析位置的判断问题)
			if msg[0] == '*' { // 语法规则
				expectedLine, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 32)
				if err != nil {
					_, _ = client.conn.Write(UnknownErrReplyBytes)
					continue // 接着后边收 这样好吗?
				}
				/*
					下面是值得借鉴之处, 怎么阻止直接关闭连接 waitGroup
					然后在close中wait即可，还可以学这里加timeout,一定时间限制
				 */
				client.WaitingReply.Add(1)  //正在传输数据防止直接关闭
				client.isRecving.Set(true)	// 为了正确解析一条指令

				// 初始化缓冲区计数器等
				client.expectedArgsCount = uint32(expectedLine)
				client.recivedCount = 0
				client.args = make([][]byte, expectedLine)
			} else {
				// text protocol error
				// TODO
			}

		} else {
			// 收到剩余的也就是后面的字段
			line := msg[0:len(msg)-2]
			if line[0] == '$' {
				// 长度部分
				fixedLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
				if err != nil {
					errReply := &reply.ProtocolErrReply{Msg:err.Error()}
					_, _ = client.conn.Write(errReply.ToBytes())
				}
				if fixedLen <=0 {
					errReply := &reply.ProtocolErrReply{Msg: "invalid multibulk length"}
					_, _ = client.conn.Write(errReply.ToBytes())				}
			} else {
				//参数部分
				client.args[client.recivedCount] = line
				client.recivedCount++
			}

			// 还要看是否已经发完
			if client.recivedCount == client.expectedArgsCount {
				// 状态改变
				client.isRecving.Set(false)


				// 中间还要执行命令(还可以开一个协程)
				result := h.db.Exec(client, client.args)
				if result != nil {
					_, _ = conn.Write(result.ToBytes())
				} else {
					_, _ = conn.Write(UnknownErrReplyByte)
				}
				// Exec之前还不能Done以及将args变为空


				client.WaitingReply.Done()

				client.expectedArgsCount = 0
				client.recivedCount = 0
				client.args = nil
			}
		}
	}
}

func (h *Handler)Close() error {
	logger.Info("handler shuting down...")
	// listener出错后能够判断是要关闭了,
	// 对Client好像没用, 主要是在listener出错时好判断
	// 就是说handler.Close()不仅设置listener的关闭标识
	// 最重要的是关闭所有Client
	h.closing.Set(true)

	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*Client)
		client.Close()
		return true
	})
	h.db.Close()
	return nil
}

