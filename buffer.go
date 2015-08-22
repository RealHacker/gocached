package main

import (
	"sync"
	"hash/fnv"
	"time"
	"fmt"
	"errors"
	"container/list"
)

type ValueItem struct {
	key string
	value *[]byte
	flags int64
	accessTime int64
	ttl int64
	expireTime int64
}

const BUCKETS_CNT int = 1024 //TODO: add a global setting for cache bucket count
const BUCKET_MAX_SIZE int = 1024

// Use a bucket to hold all conflicting items, use a mutex to guard access
type CacheBucket struct {
	lru *list.List
	items map[string]*list.Element //wrap ValueItem in list.Element for LRU 
	sync.RWMutex
}
// The cache is implemented with an array of buckets, the index being the result of hash function on keys
type CacheStore []*CacheBucket

func (st CacheStore) String() string {
	result := "*****************\r\n"
	for _, bucket := range st {
		if len(bucket.items)==0{
			continue
		}
		lru := *bucket.lru
		lruLine := "["
		for e := lru.Front(); e != nil; e = e.Next() {
			lruLine += e.Value.(ValueItem).key+" "
		}
		lruLine+="]"
		result += "LRU:"+lruLine +"\n"
		for k,v := range bucket.items {
			item := v.Value.(ValueItem)
			line := fmt.Sprintf("%s -> %s, flags: %d, expires: %d", k, item.value, item.flags, item.expireTime)
			result += line+"\n"
		}
		result += "*****************\r\n"
	}
	return result
}

func InitializeCache() CacheStore{
	store = make(CacheStore, BUCKETS_CNT) 
	for i := range store {
		store[i] = &CacheBucket{
			lru: list.New(),
			items: make(map[string]*list.Element),
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

func Get(key string) (*[]byte, int64){
	// compute the bucket number
	index := getBucketIndex(key)
	bucket := store[index]
	// lock the bucket
	bucket.Lock()
	defer bucket.Unlock()
	// get the value from bucket
	element, ok := bucket.items[key]
	if !ok {
		// if not present, return miss
		return nil, 0
	}
	item := element.Value.(ValueItem)
	// if expired, remove the element, and return miss
	now := time.Now().Unix()
	if item.expireTime < now {
		delete(bucket.items, key)
		bucket.lru.Remove(element)
		return nil, 0
	}
	// Just used, move to LRU front
	bucket.lru.MoveToFront(element)
	// update access time
	item.accessTime = now
	item.expireTime = item.accessTime + item.ttl

	fmt.Println(store)
	// return value
	return item.value, item.flags
}

func Set(key string, value *[]byte, ttl int64, flags int64) error {
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
		flags: flags,
		accessTime: time.Now().Unix(),
		ttl: ttl,
	}
	if ttl != 0 {
		item.expireTime = item.accessTime + item.ttl
	} else{
		item.expireTime = -1
	}

	element, ok := bucket.items[key]
	if ok {
		element.Value = item
		bucket.lru.MoveToFront(element)		
	}else{
		element := bucket.lru.PushFront(item)
		bucket.items[key] = element
		for bucket.lru.Len() > BUCKET_MAX_SIZE {
			// remove the oldest element when exceeding bucket limit
			oldest := bucket.lru.Back()
			if oldest != nil {
				item := oldest.Value.(ValueItem)
				delete(bucket.items, item.key)
				bucket.lru.Remove(oldest)
			}
		}
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
	element, ok := bucket.items[key];
	if !ok{
		// if not present, return miss
		return errors.New("key not found")
	}
	//drop memory on the floor for gc 
	item := element.Value.(ValueItem)
	item.value = nil

	delete(bucket.items, key)
	bucket.lru.Remove(element)

	fmt.Println(store)
	return nil
}

func Touch(key string, exptime int64) error {
	// get the bucket
	index := getBucketIndex(key)
	bucket := store[index]
	// lock the bucket
	bucket.Lock()
	defer bucket.Unlock()

	element, ok := bucket.items[key];
	if !ok{
		// if not present, return miss
		return errors.New("key not found")
	}
	// update time fields
	item := element.Value.(ValueItem)
	item.accessTime = time.Now().Unix()
	item.ttl = exptime
	item.expireTime = item.accessTime + exptime
	// refresh lru
	bucket.lru.MoveToFront(element)	
	fmt.Println(store)
	return nil
}