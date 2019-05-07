package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/EndFirstCorp/onedb"
	"github.com/garyburd/redigo/redis"
)

var errInvalidRedisQueryType = errors.New("Invalid query. Must be of type *RedisCommand")
var errInvalidRedisExecType = errors.New("Invalid execute request. Must be of type *RedisCommand")

type newConnPoolFunc func(string, int, string, int, int) pooler

var redisCreate newConnPoolFunc = newConnPool

func newConnPool(server string, port int, password string, maxIdle, maxConnections int) pooler {
	const timeout = 2 * time.Second
	return &redis.Pool{
		MaxIdle:   maxIdle,
		MaxActive: maxConnections,
		Dial: func() (redis.Conn, error) {
			tc, err := onedb.DialTCP("tcp", fmt.Sprintf("%s:%d", server, port))
			if err != nil {
				return nil, err
			}
			c := redis.NewConn(tc, timeout, timeout)
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

// Rediser is the public interface for querying Redis
type Rediser interface {
	Close() error
	Del(key string) error
	Do(command string, args ...interface{}) (interface{}, error)
	Get(key string) (string, error)
	GetStruct(key string, result interface{}) error
	SetWithExpire(key string, value interface{}, expireSeconds int) error
}

type pooler interface {
	Close() error
	Get() redis.Conn
}

type redisBackend struct {
	pool pooler
}

// New is the constructor for a Redis connection
func New(server string, port int, password string, maxIdle, maxConnections int) Rediser {
	return &redisBackend{newConnPool(server, port, password, maxIdle, maxConnections)}
}

func (r *redisBackend) Close() error {
	return r.pool.Close()
}

func (r *redisBackend) Get(key string) (string, error) {
	return redis.String(r.Do("GET", key))
}

func (r *redisBackend) GetStruct(key string, result interface{}) error {
	data, err := redis.Bytes(r.Do("GET", key))
	if err != nil {
		return err
	}
	return json.Unmarshal(data, result)
}

func (r *redisBackend) SetWithExpire(key string, value interface{}, expireSeconds int) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = r.Do("SETEX", key, expireSeconds, string(data))
	return err
}

func (r *redisBackend) Del(key string) error {
	_, err := r.Do("DEL", key)
	return err
}

func (r *redisBackend) Do(command string, args ...interface{}) (interface{}, error) {
	c := r.pool.Get()
	defer c.Close()

	return c.Do(command, args...)
}
