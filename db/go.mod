module db

go 1.14

require (
	github.com/HDT3213/godis v0.0.0-20210206023552-6edf756d45b2
	github.com/go-redis/redis v6.15.9+incompatible // indirect
	redis.simple v0.0.0
)

replace redis.simple => ../../Redis-simple
