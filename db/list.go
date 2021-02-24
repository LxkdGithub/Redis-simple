package db

import (
	"github.com/HDT3213/godis/src/datastruct/list"
	"github.com/HDT3213/godis/src/redis/reply"
)

func (db *DB)getList(key string) (*list.LinkedList, reply.ErrorReply) {
	entity, ok := db.GET(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.(*list.LinkedList)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}
