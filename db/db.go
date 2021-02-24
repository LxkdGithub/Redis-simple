//+build linux

// AOF (append only file)日志追加
// 通过异步的记录文件能保存最新的数据库状态
// 以便恢复
// 其中，关键处理好重写，不然aof文件会越来越大
// 新的aof文件能减少冗余命令
//还有，要最客观地记录，比如expire 命令转换为expireat命令
// 那么在重写过程中的请求呢? 怎么保持同步?
// 所以这里分为现有现有AOF文件和AOF重写缓冲区，
// 命令先写入现有AOF文件(同步位置), 再写入AOF重写缓冲（）
// 在aof文件重写之后直接将aof缓冲区内容写入新的aof文件，只有这里才是阻塞服务的，
// 能最大保持可用性，所以没有什么是加一层不能解决的


package db

import (
	"container/list"
	"fmt"
	"github.com/LxkdGithub/Code/Redis-simple/config"
	"github.com/LxkdGithub/Redis-simple/interface/redis"
	"github.com/LxkdGithub/Redis-simple/lib/logger"
	"github.com/LxkdGithub/Redis-simple/pubsub"
	"github.com/LxkdGithub/Redis-simple/redis/"
	"os"
	"github.com/LxkdGithub/Redis-simple/datastruct/dict"
	"github.com/LxkdGithub/Redis-simple/datastruct/lock"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type DB struct {
	// 存储
	Data dict.Dict
	TTLMap dict.Dict

	SubMap dict.Dict

	// 这是用来存放list中block的key的
	blockKeys map[string][]*server.Client		// map { "key" => []server }
	Locker *lock.Locks

	// TimerTask interval
	interval time.Duration

	stopWorld sync.WaitGroup

	hub *pubsub.Hub


	// 命令发送介质(将需要记录的命令发送过去）
	aofChan chan *reply.MultiBulkReply
	// append file 文件描述符
	aofFile *os.File

	aofFilename string

	// 重写chan 通知主协程(处理协程)稍加改变写到缓冲去
	aofRewriteChan chan *reply.MultiBulkReply
	// 暂停操作
	pausingAof sync.RWMutex
}

type extra struct {
	// 是否需要持久化 比如失败的命令就不需要了
	toPersist bool
	// 持久化的特殊信息
	specialAof []*reply.MultiBulkReply

}

var router = MakeRouter()

func MakeDB() *DB {
	db := &DB{
		Data: dict.MakeConcurrent(dataDictSize),
		TTLMap: dict.MakeConcurrent(ttlDictSize),
		Locker: lock.Make(lockerSize),
		interval: 5 * time.Second,
		hub: pubsub.MakeHub(),
	}

	if cofig.Properties.AppendOnly {
		db.aofFilename = config.Properties.AppendFilename
		db.loadAof(0)
		aofFile, err := os.Open(db.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			logger.Warn(err)
		} else {
			db.aofFile = aofFile
			db.aofChan = make(chan *reply.MultiBulkReply, aofQueueSize)
		}
		go func() {
			db.HandleAof()  // Aof主协程(里边在重写时开启另一个协程?是不是?)
		}()
	}

	// start timer worker
	db.TimerTask()
	return db
}

func (db *DB)Close() {
	if db.aofFile != nil {
		err := db.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
}


func (db *DB)Exec(c redis.Connection, args [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(args[0]))

	// 先处理特殊命令
	if cmd == "subscribe" {
		if len(args) < 2 {
			return &reply.ArgNumErrReply{Cmd: "subscribe"}
		}
		return pubsub.Subscribe(db.hub, c, args[1:])
	} else if cmd == "publish" {
		return pubsub.Publish(db.hub, args[1:])
	} else if cmd == "unsubscribe" {
		return pubsub.UnSubscribe(db.hub, c, args[1:])
	} else if cmd == "bgrewriteaof" {
		reply := BGRewriteAOF(db, args[1:])
		return reply
	}


	// normal cmd
	cmdFunc, ok := router[cmd]
	if !ok {
		return reply.MakeErrReply("Err unknown command '" + cmd + "'")
	}
	if len(args) > 1 {
		result = cmdFunc(db, args[1:])
	} else {
		// 不需要参数
		result = cmdFunc(db, [][]byte{})
	}

	// 这里本该得到是否成功然后aof的
	// TODO

	return
}


// 具体方法
// Data access
func (db *DB)GET(key string) (*DataEntity, bool) {
	// 操作之前先wait，wait是为了避免退出而出意外吧
	// 为什么只有wait而没有Done呢
	db.stopWorld.Wait()

	if db.IsExpired(key) {
		return nil, false
	}
	raw, ok := db.Data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*DataEntity)
	return entity, true
}

func (db *DB)PUT(key string, entity *DataEntity) int {
	db.stopWorld.Wait()
	return db.Data.Put(key, entity)
}

// PutIfExists是指只有存在才放进去
func (db *DB)PutIfExists(key string, entity *DataEntity) int {
	db.stopWorld.Wait()
	return db.Data.PutIfExists(key, entity)
}

func (db *DB)PUTIfAbsent(key string, entity *DataEntity) int {
	db.stopWorld.Wait()
	return db.Data.PutIfAbsent(key, entity)
}

func (db *DB)Remove(key string) {
	db.stopWorld.Wait()
	db.Data.Remove(key)
	db.TTLMap.Remove(key)
}

func (db *DB)Removes(keys ...string) (deleted int) {
	db.stopWorld.Wait()
	deleted = 0
	for _, key := range keys {
		if _, exists := db.Data.GET(key); exists {
			db.Data.Remove(key)
			db.TTLMap.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB)Flush() {
	// Flush 时不能操作
	db.stopWorld.Add(1)
	defer db.stopWorld.Done()

	db.Data = dict.MakeConcurrent(dataDictSize)
	db.TTLMap = dict.MakeConcurrent(ttlDictSize)
	db.Locker = lock.Make(lockerSize)
}

// ---- Lock Function ---
// DB层面也需要Lock 刚才的只是基本操作

func (db *DB)Lock(key string) {
	db.Locker.Lock(key)
}

func (db *DB) RLock(key string) {
	db.Locker.RLock(key)
}

func (db *DB) UnLock(key string) {
	db.Locker.UnLock(key)
}

func (db *DB) RUnLock(key string) {
	db.Locker.RUnLock(key)
}

func (db *DB) Locks(keys ...string) {
	db.Locker.Locks(keys...)
}

func (db *DB) RLocks(keys ...string) {
	db.Locker.RLocks(keys...)
}

func (db *DB) UnLocks(keys ...string) {
	db.Locker.UnLocks(keys...)
}

func (db *DB) RUnLocks(keys ...string) {
	db.Locker.RUnLocks(keys...)
}

// --- TTL Functions ---
// 怎么实现过期(静态还是动态?)
// 1.开启一个计时器协程(即使这样，操作时还是要再看一次是否过期)
// 2.操作时删除过期key
func (db *DB)Expire(key string, expireTime time.Time) {
	db.stopWorld.Wait()
	db.TTLMap.Put(key, expireTime)
}

func (db *DB)Persist(key string) {
	db.stopWorld.Wait()
	db.TTLMap.Remove(key)
}

func (db *DB)IsExpired(key string) bool {
	rawExpireTime, ok := db.TTLMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

// 难道说两种结合吗(定时使用CleanExpired函数进行清除)
func (db *DB)CleanExpired() {
	now := time.Now()
	// 因为foreach无法删除遍历中的key，所以要找一个集合暂时存储起来
	// 这个集合还不能有大小限制(要么使用链表，要么先放进切片后append)
	toRemve := list.List{}
	db.TTLMap.ForEach(func(key string, val interface{}) bool {
		expireTime, _ := val.(time.Time)
		if now.After(expireTime) {
			db.Data.Remove(key)
			toRemve.PushBack(key)
		}
		return true
	})
	for p := toRemve.Front(); p != nil; p = p.Next() {
		db.TTLMap.Remove(p.Value.(string))
	}
}

// 每隔interval秒clean一次
func (db *DB)Timertask() {
	ticker := time.NewTicker(db.interval)
	go func() {
		for range ticker.C {
			db.CleanExpired()
		}
	}()
}

func (db *DB)AfterClientClose(c redis.Connection) {
	pubsub.UnsubscribeAll(db.hub, c)
}






