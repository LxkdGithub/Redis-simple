package pubsub

import (
	"redis.simple/datastruct/dict"
	"redis.simple/datastruct/list"
	"redis.simple/interface/redis"
	"redis.simple/redis/reply"
	"strconv"
)

var (
	_subscribe = "subscribe"
	_unsubscribe = "unsubscribe"
	messageBytes = []byte("message")
	unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n")
)

func makeMsg(t string, channel string, code int64) []byte {
	return []byte("*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + reply.CRLF + t + reply.CRLF +
		"$" + strconv.FormatInt(int64(len(channel)), 64) + reply.CRLF + channel + reply.CRLF +
		":" + strconv.FormatInt(code, 10) + reply.CRLF)
}

/*
 * invoker should lock channel
 * return: is new subscribed
 */
func subscribe0(hub *Hub, channel string, client redis.Connection) bool {
	client.SubChanel(channel)

	// add to Hub
	raw, ok := hub.subs.Get(channel)
	var subscribers *list.LinkedList
	if ok {
		subscribers, _ = raw.(*list.LinkedList)
	} else {
		subscribers = list.Make()
		hub.subs.Put(channel, subscribers, dict.LIST)
	}
	if subscribers.Contains(client) {
		return false
	}
	subscribers.Lpush(client)     // 不必返回元素个数
	return true
}

func unsubscribe0(hub *Hub, channel string, client redis.Connection) bool {
	client.UnSubsChannel(channel)

	raw, ok := hub.subs.Get(channel)
	if ok {
		subscribers, _ := raw.(list.LinkedList)
		subscribers.Lrem(-1, client)  // -1代表全部

		if subscribers.Llen() == 0 {
			hub.subs.Remove(channel)
		}
		return true
	}
	return false
}

func Subscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	for i, b := range args {
		channels[i] = string(b)
	}

	hub.subsLocker.Locks(channels...)	// 把这些channel锁住
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		if subscribe0(hub, channel, c) {
			_ = c.Write(makeMsg(_subscribe, channel, int64(c.SubsCount())))
		}
	}
	return &reply.NoReply{}
}

func UnSubscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	// 如果args为空 就unsubscribe全部channels
	var channels []string
	if len(args) == 0 {
		channels = c.GetChannels()
	} else {
		channels = make([]string, len(args))
		for i, b := range args {
			channels[i] = string(b)
		}
	}

	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	// nothing to unsubscribe
	if len(channels) == 0 {
		_ = c.Write([]byte(unSubscribeNothing))
		return &reply.NoReply{}
	}

	for _, channel := range channels {
		if unsubscribe0(hub, channel, c) {
			// 按照Redis 的通信协议返回结果 "unsubscribe" + channelName + subCount
			_ = c.Write(makeMsg(_unsubscribe, channel, int64(c.SubsCount())))
		}
	}
	// 正常返回(无特殊情况)
	return &reply.NoReply{}  // 指针实现的接口，自然要返回指针类型
}

func UnSubscribeAll(hub *Hub, c redis.Connection) redis.Reply {
	return UnSubscribe(hub, c, [][]byte{})
}

func Publish(hub *Hub, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return &reply.ArgNumErrReply{Cmd: "publish"}
	}
	channel := string(args[0])
	message := args[1]

	hub.subsLocker.Lock(channel)
	defer hub.subsLocker.UnLock(channel)

	raw, ok := hub.subs.Get(channel)
	if !ok {
		return reply.MakeIntReply(0)
	}
	subscribers, _ := raw.(list.LinkedList)
	subscribers.Foreach(func(i int, conn interface{}) bool {
		client, _ := conn.(redis.Connection)
		replyArgs := make([][]byte, 3)
		replyArgs[0] = messageBytes			// messageType
		replyArgs[1] = []byte(channel)
		replyArgs[2] = message
		_ = client.Write(reply.MakeMultiBulkReply(replyArgs).ToBytes())
		return true
	})
	// Reply用于内部通信之用
	return reply.MakeIntReply(int64(subscribers.Llen()))
}

