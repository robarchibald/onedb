package onedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/EndFirstCorp/onedb"
	"github.com/garyburd/redigo/redis"
)

var errInvalidRedisQueryType = errors.New("Invalid query. Must be of type *RedisCommand")
var errInvalidRedisExecType = errors.New("Invalid execute request. Must be of type *RedisCommand")

var redisCreate redisCreator = &redisRealCreator{}

type redisCreator interface {
	newConnPool(server string, port int, password string, maxIdle, maxConnections int) redisBackender
}

type redisRealCreator struct{}

func (c *redisRealCreator) newConnPool(server string, port int, password string, maxIdle, maxConnections int) redisBackender {
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

type RedisCommand struct {
	Command string
	Args    []interface{}
}

type redisBackender interface {
	Close() error
	Get() redis.Conn
}

type redisBackend struct {
	pool redisBackender
}

func NewRedis(server string, port int, password string, maxIdle, maxConnections int) onedb.DBer {
	return &redisBackend{redisCreate.newConnPool(server, port, password, maxIdle, maxConnections)}
}

func NewRedisGetCommand(key string) *RedisCommand {
	return &RedisCommand{Command: "GET", Args: []interface{}{key}}
}

func NewRedisDelCommand(key string) *RedisCommand {
	return &RedisCommand{Command: "DEL", Args: []interface{}{key}}
}

func NewRedisSetCommand(key string, value interface{}, expireSeconds int) (*RedisCommand, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &RedisCommand{Command: "SETEX", Args: []interface{}{key, strconv.Itoa(expireSeconds), string(data)}}, nil
}

func (r *redisBackend) Backend() interface{} {
	return r.pool
}

func (r *redisBackend) Close() error {
	return r.pool.Close()
}

func (r *redisBackend) Do(command string, args ...interface{}) (interface{}, error) {
	c := r.pool.Get()
	defer c.Close()

	return c.Do(command, args...)
}

func (r *redisBackend) QueryValues(query *onedb.Query, result ...interface{}) error {
	return nil
}

func (r *redisBackend) QueryJSON(command string, args ...interface{}) (string, error) {
	return redis.String(r.Do(command, args...))
}

func (r *redisBackend) QueryJSONRow(command string, args ...interface{}) (string, error) {
	return r.QueryJSON(command, args...)
}

func (r *redisBackend) QueryStruct(result interface{}, command string, args ...interface{}) error {
	data, err := redis.Bytes(r.Do(command, args...))
	if err != nil {
		return err
	}
	return json.Unmarshal(data, result)
}

func (r *redisBackend) QueryStructRow(result interface{}, command string, args ...interface{}) error {
	return r.QueryStruct(result, command, args...)
}
