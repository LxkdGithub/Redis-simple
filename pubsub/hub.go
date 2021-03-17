package pubsub

import (
	"redis.simple/datastruct/lock"
	"redis.simple/datastruct/dict"
)

type Hub struct {
	// keys -> list(*Client)  这里使用了List结构
	subs dict.Dict
	// lock channel
	subsLocker *lock.Locks
}

func MakeHub() *Hub {
	return &Hub{
		subs: dict.MakeConcurrent(4),
		subsLocker: lock.Make(16),
	}
}
