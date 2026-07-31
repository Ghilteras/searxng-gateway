package cache

import (
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

type entry struct {
	val any
	ts  time.Time
}

type Cache struct {
	lru *lru.Cache[string, entry]
	ttl time.Duration
}

func New(size int, ttl time.Duration) (*Cache, error) {
	l, err := lru.New[string, entry](size)
	if err != nil {
		return nil, err
	}
	return &Cache{lru: l, ttl: ttl}, nil
}

func (c *Cache) Get(key string) (any, bool) {
	e, ok := c.lru.Get(key)
	if !ok {
		return nil, false
	}
	if c.ttl > 0 && time.Since(e.ts) > c.ttl {
		c.lru.Remove(key)
		return nil, false
	}
	return e.val, true
}

func (c *Cache) Set(key string, val any) {
	c.lru.Add(key, entry{val: val, ts: time.Now()})
}

func (c *Cache) Len() int {
	return c.lru.Len()
}
