package main

import (
	"sync"
	"hash/fnv"
	"time"
	"fmt"
	"errors"
)

type ValueItem struct {
	key string
	value *[]byte
	
	accessTime int64
	ttl int64
	expireTime int64
	isExpired bool

	isDeleted bool
}

const BUCKETS_CNT int = 1024 //TODO: add a global setting for cache bucket count

// Use a bucket to hold all conflicting items, use a mutex to guard access
type CacheBucket struct {
	items map[string]ValueItem
	sync.RWMutex
}
// The cache is implemented with an array of buckets, the index being the result of hash function on keys
type CacheStore []*CacheBucket

func InitializeCache() CacheStore{
	store = make(CacheStore, BUCKETS_CNT) 
	for i := range store {
		store[i] = &CacheBucket{
			items: make(map[string]ValueItem),
		}
	}
	return store
}

var store CacheStore

func init(){
	store = InitializeCache()
}

func getBucketIndex(key string) uint{
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return uint(hasher.Sum32())%uint(BUCKETS_CNT)
}

func Get(key string) *[]byte{
	// compute the bucket number
	index := getBucketIndex(key)
	bucket := store[index]
	// lock the bucket
	bucket.Lock()
	defer bucket.Unlock()
	// get the value from bucket
	item, ok := bucket.items[key]
	if !ok {
		// if not present, return miss
		return nil
	}
	// if deleted, return miss
	if item.isDeleted {
		return nil
	}
	// update access time
	item.accessTime = time.Now().Unix()
	item.expireTime = item.accessTime + item.ttl

	fmt.Println(store)
	// return value
	return item.value
}

func Set(key string, value *[]byte, ttl int64) error {
	// get the bucket
	index := getBucketIndex(key)
	bucket := store[index]
	// lock the bucket
	bucket.Lock()
	defer bucket.Unlock()
	// set the value
	item := ValueItem {
		key: key,
		value: value,
		accessTime: time.Now().Unix(),
		ttl: ttl,
	}
	
	bucket.items[key] = item
	// update access time
	if ttl != 0 {
		item.expireTime = item.accessTime + item.ttl
	} else{
		item.expireTime = -1
	}
	fmt.Println(store)
	return nil
}

func Delete(key string) error {
	// get the bucket
	index := getBucketIndex(key)
	bucket := store[index]
	// lock the bucket
	bucket.Lock()
	defer bucket.Unlock()
	// delete the value
	item, ok := bucket.items[key];
	if !ok{
		// if not present, return miss
		return errors.New("key not found")
	}
	//drop memory on the floor for gc 
	item.value = nil
	delete(bucket.items, key)

	fmt.Println(store)
	return nil
}