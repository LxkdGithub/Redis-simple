module datastruct

go 1.14

require redis.simple/datastruct v0.0.0
require redis.simple/redis v0.0.0
require redis.simple/lib v0.0.0
require redis.simple/db v0.0.0

replace redis.simple/datastruct => ../datastruct
replace redis.simple/redis => ../redis
replace redis.simple/lib => ../lib
replace redis.simple/db => ../db