// Copyright 2018 The Redix Authors. All rights reserved.
// Use of this source code is governed by a Apache 2.0
// license that can be found in the LICENSE file.
//
// bolt is a db engine based on boltdb

package ram

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alash3al/redix/kvstore"
)

// RamDB - represents a ram db implementation
type RamDB struct {
	ram           *sync.Map
	countersLocks sync.RWMutex
}

// OpenRam - Opens the specified path
func OpenRam(path string) (*RamDB, error) {
	db := new(RamDB)
	db.ram = &sync.Map{}
	db.countersLocks = sync.RWMutex{}

	return db, nil
}

// Size - returns the size of the database
func (db *RamDB) Size() int64 {
	count := int64(0)

	db.ram.Range(func(key, value interface{}) bool {
		count += int64(len(key.(string)) + len(value.(string)))
		return true
	})

	return count
}

// GC - runs the garbage collector
func (db *RamDB) GC() error {
	return nil
}

// Incr - increment the key by the specified value
func (db *RamDB) Incr(k string, by int64) (int64, error) {
	db.countersLocks.Lock()
	defer db.countersLocks.Unlock()

	val, _ := db.Get(k)
	valInt, _ := strconv.ParseInt(val, 10, 64)
	valInt += by

	db.Set(k, strconv.FormatInt(valInt, 10), -1)

	return valInt, nil
}

// Set - sets a key with the specified value and optional ttl
func (db *RamDB) Set(k, v string, ttl int) error {
	var expires int64

	if ttl > 0 {
		expires = time.Now().Add(time.Duration(ttl) * time.Millisecond).Unix()
	}

	v = strconv.Itoa(int(expires)) + ";" + v

	db.ram.Store(k, v)

	return nil
}

// MSet - sets multiple key-value pairs
func (db *RamDB) MSet(data map[string]string) error {

	for k, v := range data {
		db.Set(k, v, -1)
	}

	return nil
}

// Get - fetches the value of the specified k
func (db *RamDB) Get(k string) (string, error) {

	var delKeys []string
	delete := false

	data, ok := db.ram.Load(k)

	if !ok {
		return "", errors.New("Key Not Found")
	}

	parts := strings.SplitN(data.(string), ";", 2)

	expires, actual := parts[0], parts[1]

	if exp, _ := strconv.Atoi(expires); exp > 0 && int(time.Now().Unix()) >= exp {
		delete = true
	}

	if delete {
		delKeys = append(delKeys, k)
		db.Del(delKeys)

		return "-2", nil
	}

	return actual, nil
}

// MGet - fetch multiple values of the specified keys
func (db *RamDB) MGet(keys []string) (data []string) {

	fmt.Println("data", data)

	for _, k := range keys {
		val, _ := db.Get(k)

		data = append(data, val)
	}

	return data
}

// TTL - returns the time to live of the specified key's value
func (db *RamDB) TTL(key string) int64 {
	val, ok := db.ram.Load(key)

	if !ok {
		return -2
	}

	if val == nil {
		return -2
	}

	parts := strings.SplitN(val.(string), ";", 2)

	exp, _ := strconv.Atoi(parts[0])
	expires := int64(exp)
	now := time.Now().Unix()

	if expires == 0 {
		return -1
	}

	if expires == -1 {
		return -1
	}

	if now >= expires {
		return -2
	}

	return (expires - now)
}

// Del - removes key(s) from the store
func (db *RamDB) Del(keys []string) error {

	for _, k := range keys {
		db.ram.Delete(k)
	}

	return nil
}

// Scan - iterate over the whole store using the handler function
func (db *RamDB) Scan(scannerOpt kvstore.ScannerOptions) error {

	return nil
}
