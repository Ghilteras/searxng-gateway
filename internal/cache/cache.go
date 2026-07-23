package cache

import (
	lru "github.com/hashicorp/golang-lru/v2"
)

type Cache struct {
	lru *lru.Cache[string, any]
}

func New(size int) (*Cache, error) {
	l, err := lru.New[string, any](size)
	if err != nil {
		return nil, err
	}
	return &Cache{lru: l}, nil
}

func (c *Cache) Get(key string) (any, bool) {
	return c.lru.Get(key)
}

func (c *Cache) Set(key string, val any) {
	c.lru.Add(key, val)
}

func (c *Cache) Len() int {
	return c.lru.Len()
}
