package db

import "Redis-simple/interface/redis"

type DB interface {
	Exec(client redis.Connection, args []byte) redis.Reply
	// afterclose之后还有工作要做，将有关client的工作清除
	AfterClientClose(c redis.Connection)
	Close()
}
