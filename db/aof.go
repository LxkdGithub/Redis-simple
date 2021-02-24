package db

import (
	"bufio"
	"datastruct/lock"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

var pExpireAtCmd = []byte("PEXPIREAT")


/*
	以下的函数将命令封装为aof命令以便持久化
 */

func makeExpireCmd(key string, expireAt time.Time) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtCmd
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return reply.MakeMultiBulkReply(args)
}

func makeAofCmd(cmd string, args [][]byte) *reply.MultiBulkReply {
	params := make([][]byte, len(args) - 1)
	copy(params[1:], args)
	params[0] = []byte(cmd)
	return reply.MakeMultiBulkReply(params)
}

// send to chan to aof
func (db *DB)AddAof(args *reply.MultiBulkReply) {
	if config.Propertites.AppendOnly && db.aofChan != nil {
		db.aofChan <- args
	}
}

// 执行完后makeAofCmd 然后放入aofChan里，HandleAof协程自然会继续处理
/*
	下面的加锁告诉我们要注意好各种意外和退出, 防止某过程中的退出
 */
func (db *DB)handleAof() {	// 在初始化db是时候就卡开启了这个协程,所以不会阻塞
	for cmd := range db.aofChan {
		db.pausingAof.RLock() // 这个锁干嘛,防止写入时被别的协程阻塞
		// 判断是否是aofRewritten 状态
		if db.aofRewriteChan != nil {
			// 不仅要写入aof文件， 还要写入aof冲了重写缓存
			db.aofRewriteChan <- cmd
		}
		_, err := db.aofFile.Write(cmd.toBytes())
		if err != nil {
			logger.Warn("Rewritten err: " + err.Error())
		}
		db.pausingAof.RUnlock()
	}
}

// ----

func trim(msg []byte) string {
	// 为什么从后边trim的
	for i := len(msg) - 2;i >= 0;i-- {
		if msg[i] == '\r' || msg[i] == '\n' {
			continue
		}
		return string(msg[:i+1])
	}
	return ""
}


// 写完之后该读取了
// 还有将aofRewrittenChan里的呢
func (db *DB)loadAof(maxBytes int) {
	// delete aofChan to prevent write again
	aofChan := db.aofChan
	db.aofChan = nil
	defer func(aofChan chan *reply.MultiBulkReply) {
		db.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(db.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var fixedLen int64 = 0
	var expectedArgsCount uint32
	var receivedCount uint32
	var args [][]byte
	processing := false
	var msg []byte
	readBytes := 0
	for {
		if maxBytes != 0 && readBytes >= maxBytes {
			break
		}
		if fixedLen == 0 {
			msg, err = reader.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					logger.Warn(err)
				}
				return
			}
			readBytes += len(msg)
		} else {
			msg = make([]byte, fixedLen+2)
			n, err := io.ReadFull(reader, msg)
			if err != nil {
				if err != io.EOF {
					logger.Warn("invalid multibulk length")
				}
				return
			}
			fixedLen = 0
			readBytes += n
		}

		if !processing {
			// new request
			if msg[0] == '*' {
				// bulk multi msg
				expectedLine, err := strconv.ParseUint(trim(msg[1:]), 10, 32)
				if err != nil {
					logger.Warn(err)
					return
				}
				expectedArgsCount = uint32(expectedLine)
				receivedCount = 0
				processing = true
				args = make([][]byte, expectedLine)
			} else {
				logger.Warn("msg should start with '*'")
				return
			}
		} else {
			// receive following part of a request
			line := msg[0 : len(msg)-2]
			if line[0] == '$' {
				fixedLen, err = strconv.ParseInt(trim(line[1:]), 10, 64)
				if err != nil {
					logger.Warn(err)
					return
				}
				if fixedLen <= 0 {
					logger.Warn("invalid multibulk length")
					return
				}
			} else {
				args[receivedCount] = line
				receivedCount++
			}

			// if sending finished
			if receivedCount == expectedArgsCount {
				processing = false

				cmd := strings.ToLower(string(args[0]))
				cmdFunc, ok := router[cmd]
				if ok {
					cmdFunc(db, args[1:])
				}

				// finish
				expectedArgsCount = 0
				receivedCount = 0
				args = nil
			}
		}
	}
}


// aofRewrite
// 重写过程:
// 1. 因为要拿到旧的aof文件副本，这里不需要副本，(所以要取文件大小，当然要锁住了)
// 只需要记录开始重写时的文件大小就可以，读到那个位置就停下
// 2.然后读取aof重写缓冲区即可(这里也要锁住)
func (db *DB)startRewrite() (*os.File, int64, error) {
	db.pausingAof.Lock()
	defer db.pausingAof.Unlock()

	db.aofRewriteChan = make(chan *reply.MultiBulkReply, aofQueueSize)

	fileInfo, _ := os.Stat(db.aofFilename)
	fileSize := fileInfo.Size()

	file, err := ioutil.TempFile("", "aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, 0, err
	}
	return file, fileSize, nil
}


// 为什么需要一个新的DB
// 是用来进行重写记录状态的
// *** 这里不知道该怎么实现 -- 就是怎么记录状态呀(神奇的SimpleDict?)
// 其实这里就是把aof文件里的所有指令重新作用到一个simpleDB, 对DB里的每个key
// 进行反推指令，比如<key, val<set>> 那就是使用persistSet重新构造出指令cmd
// 这样每个<key, val>一定只对应一条指令，可达到简化目的(看看官方怎么做?)
// adb是异步的，会有不足，所以要靠aof的同步刷新,这些指令的结果可能adb里还没有
func (db *DB)aofWrite() {
	// 三大步
	// 1. 加锁，获取aof文件状态(大小，知道rewriteBuff是哪个位置之后的)
	// 2. loadAof刷入simpleDB, 对每个key反推指令，指令写入new aodFile
	// 3. rewriteBuff 写入new aofFile
	file, fileSize, err := db.startRewrite()
	if err != nil {
		logger.Warn(err)
		return
	}

	tmpDB := &DB{
		Data: dict.MakeSimple(),
		TTLMap:   dict.MakeSimple(),
		Locker:   lock.Make(lockerSize),
		interval: 5 * time.Second,

		aofFilename: db.aofFilename,
	}
	tmpDB.loadAof(int(fileSize))  // 将现有状态导入tmpDB

	tmpDB.Data.ForEach(func(key string, raw interface{}) bool {
		var cmd *reply.MultiBulkReply
		entity, _ := raw.(*DataEntity)
		switch val := entity.Data.(type) { // 为什么不直接raw.(type)
		case []byte:
			cmd = persistString(key, val)	// 重点在这里(变为指令?)
		case *List.LinkedList:
			cmd = persistList(key, val)
		case *set.Set:
			cmd = persistSet(key, val)
		case dict.Dict:
			cmd = persistHash(key, val)
		case *SortedSet.SortedSet:
			cmd = persistZSet(key, val)
		}
		if cmd != nil {
			_, _ = file.Write(cmd.ToBytes())
		}
		return true
	})
	tmpDB.TTLMap.ForEach(func(key string, raw interface{}) bool {
		expireTime, _ := raw.(time.Time)
		cmd := makeExpireCmd(key, expireTime)
		if cmd != nil {
			_, _ = file.Write(cmd.ToBytes())
		}
		return true
	})

	// aofRewriteBuff的写入
	db.finishRewrite(file)
}

var setCmd = []byte("SET")

func persistString(key string, bytes []byte) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return reply.MakeMultiBulkReply(args)
}

var rPushAllCmd = []byte("RPUSHALL")

func persistList(key string, list *List.LinkedList) *reply.MultiBulkReply {
	args := make([][]byte, 2 + list.Len())  // 参数个数未知
	args[0] = rPushAllCmd
	args[1] = []byte(key)
	list.ForEach(func(i int, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[i+2] = bytes
		return true
	})
	return reply.MakeMultiBulkReply(args)
}

var sAddCmd = []byte("SADD")

func persistSet(key string, set *set.Set) *reply.MultiBulkReply {
	args := make([][]byte, 2 + set.Len())
	args[0] = sAddCmd
	args[1] = []byte(key)
	i := 0
	set.ForEach(func(val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i] = bytes
		i++
		return true
	})
	return reply.MakeMultiBulkReply(args)
}

var hMSetCmd = []byte("HMSET")

func persistHash(key string, hash dict.Dict) *reply.MultiBulkReply {
	args := make([][]byte, 2+hash.Len()*2)
	args[0] = hMSetCmd
	args[1] = []byte(key)
	i := 0
	hash.ForEach(func(field string, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i*2] = []byte(field)
		args[3+i*2] = bytes
		i++
		return true
	})
	return reply.MakeMultiBulkReply(args)
}


var zAddCmd = []byte("ZADD")

func persistZSet(key string, zset *SortedSet.SortedSet) *reply.MultiBulkReply {
	args := make([][]byte, 2 + zet.Len() * 2)
	args[0] = zAddCmd
	args[1] = []byte(key)
	i := 0
	zset.ForEach(int64(0), int64(zset.Len()), true, func(element *SortedSet.Element) bool {
		value := strconv.FormatFloat(element.Score, 'f', -1, 64)
		args[2+i*2] = []byte(value)
		args[3+i*2] = []byte(element.Member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(args)
}



func (db *DB)finishRewrite(tmpFile *os.File) {
	// 开头结尾都要lock, 这里是结尾
	db.pausingAof.Lock()
	defer db.pausingAof.Unlock()

loop:
	for {
		select {
		case cmd <-db.aofRewriteChan:
			_, err := tmpFile.Write(cmd.ToBytes())
			if err != nil {
				logger.Warn(err)
			}
		default:
			break loop// 跳出两层循环
		}
	}

	close(db.aofRewriteChan)
	db.aofRewriteChan = nil

	// start to replace
	_ = db.aofFile.Close()
	_ = os.Rename(tmpFile.Name(), db.aofFilename)
	// 重新打开
	aofFile, err := os.OpenFile(db.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	db.aofFile = aofFile
}