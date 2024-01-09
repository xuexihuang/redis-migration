package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	// 定义命令行参数
	var addr string
	var password string
	var db int

	flag.StringVar(&addr, "h", "localhost:6379", "Redis host and port")
	flag.StringVar(&password, "a", "", "Redis password")
	flag.IntVar(&db, "d", 0, "Redis database number")
	// 解析命令行参数
	flag.Parse()
	fmt.Println("add:", addr)
	fmt.Println("password:", password)
	fmt.Println("db:", db)
	// 连接到 Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,     // Redis 地址
		Password: password, // Redis 密码，没有则留空
		DB:       0,        // 使用默认 DB
	})

	ctx := context.Background()
	file, err := os.Create("baken.sql")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	iter := rdb.Scan(ctx, 0, "", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		dumpData(ctx, rdb, key, file)
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
}

func dumpData(ctx context.Context, rdb *redis.Client, key string, file *os.File) {
	// 获取键的类型
	t := rdb.Type(ctx, key).Val()
	var cmd string
	// 根据类型处理不同的命令
	switch t {
	case "string":
		val := rdb.Get(ctx, key).Val()
		cmd = fmt.Sprintf("SET %s %s\n", key, val)
	case "list":
		val, err := rdb.LRange(ctx, key, 0, 10000).Result()
		if err == nil {
			for _, v := range val {
				tCmd := fmt.Sprintf("RPUSH %s %q\n", key, v)
				cmd += tCmd
			}
		} else {
			fmt.Println("rdb.LRange error:", err)
		}
	case "set":
		val, err := rdb.SMembers(ctx, key).Result()
		if err == nil {
			var tVal string
			for _, v := range val {
				tVal = tVal + " " + v
			}
			tCmd := fmt.Sprintf("SADD %s%s\n", key, tVal)
			cmd += tCmd
		} else {
			fmt.Println("rdb.SMembers error:", err)
		}
	case "zset":
		val, err := rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err == nil {
			for _, v := range val {
				tCmd := fmt.Sprintf("ZADD %s %.0f %s\n", key, v.Score, v.Member.(string))
				cmd += tCmd
			}

		} else {
			fmt.Println("rdb.ZRangeWithScores error:", err)
		}
	case "hash":
		val, err := rdb.HKeys(ctx, key).Result()
		if err == nil {
			for _, v := range val {
				tval, err := rdb.HGet(ctx, key, v).Result()
				if err == nil {
					var tCmd string
					if tval == "" {
						tCmd = fmt.Sprintf("HSET %s %s ", key, v)
						tCmd = tCmd + `""` + "\n"
					} else {
						tCmd = fmt.Sprintf("HSET %s %s %q\n", key, v, tval)
					}
					cmd += tCmd
				}
			}
		} else {
			fmt.Println("rdb.HKeys error:", err)
		}
	}

	// 写入到文件
	if _, err := file.WriteString(cmd); err != nil {
		panic(err)
	}

	// 获取过期时间
	ttl := rdb.TTL(ctx, key).Val()
	if ttl > 0 {
		expireCmd := fmt.Sprintf("EXPIRE %s %d\n", key, ttl/time.Second)
		if _, err := file.WriteString(expireCmd); err != nil {
			panic(err)
		}
	}
}
