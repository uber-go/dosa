// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package memory

import (
	"bytes"
	"context"
	"encoding/gob"
	"sort"
	"sync"
	"time"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
)

// Connector is an in memory connector.
// The in-memory connector stores it's data like this:
// map[string]map[string][]map[string]dosa.FieldValue
//
// the first 'string' is the table name (entity name)
// the second 'string' is the partition key, encoded using encoding/gob to guarantee uniqueness
// within each 'partition' you have a list of rows ([]map[string]dosa.FieldValue)
// these rows are kept ordered so that reads are lightning fast and searches are quick too
// the row itself is a map of field name to value (map[string]dosaFieldValue])
//
// A read-write mutex lock is used to control concurrency, making reads work in parallel but
// writes are not. There is no attempt to improve the concurrency of the read or write path by
// adding more granular locks.
type Connector struct {
	base.Connector
	data map[string]map[string][]map[string]dosa.FieldValue
	lock sync.RWMutex
}

// partitionKeyBuilder extracts the partition key components from the map and encodes them,
// generating a unique string. It uses the encoding/gob method to make a byte array as the
// key, and returns this as a string
func partitionKeyBuilder(ei *dosa.EntityInfo, values map[string]dosa.FieldValue) string {
	encodedKey := bytes.Buffer{}
	encoder := gob.NewEncoder(&encodedKey)
	for _, k := range ei.Def.Key.PartitionKeys {
		_ = encoder.Encode(values[k])
	}
	return string(encodedKey.Bytes())
}

// findInsertionPoint locates the place within a partition where the data belongs.
// It inspects the clustering key values found in the insertMe value and figures out
// where they go in the data slice. It doesn't change anything, but it does let you
// know if it found an exact match or if it's just not there. When it's not there,
// it indicates where it is supposed to get inserted
func findInsertionPoint(ei *dosa.EntityInfo, data []map[string]dosa.FieldValue, insertMe map[string]dosa.FieldValue) (found bool, idx int) {
	found = false
	idx = sort.Search(len(data), func(offset int) bool {
		cmp := compareRows(ei, data[offset], insertMe)
		if cmp == 0 {
			found = true
		}
		return cmp >= 0
	})
	return
}

// compareRows compares two maps of row data based on clustering keys. It handles ascending/descending
// based on the passed-in schema
func compareRows(ei *dosa.EntityInfo, v1 map[string]dosa.FieldValue, v2 map[string]dosa.FieldValue) (cmp int8) {
	keys := ei.Def.Key.ClusteringKeys
	for _, key := range keys {
		d1 := v1[key.Name]
		d2 := v2[key.Name]
		cmp = compareType(d1, d2)
		if key.Descending {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return cmp
}

// compareType compares a single DOSA field based on the type. This code assumes the types of each
// of the columns are the same, or it will panic
func compareType(d1 dosa.FieldValue, d2 dosa.FieldValue) int8 {
	switch d1.(type) {
	case dosa.UUID:
		// TODO: compare timestamp UUIDs
		if string(d1.(dosa.UUID)) == string(d2.(dosa.UUID)) {
			return 0
		}
		if string(d1.(dosa.UUID)) < string(d2.(dosa.UUID)) {
			return -1
		}
		return 1
	case string:
		if d1.(string) == d2.(string) {
			return 0
		}
		if d1.(string) < d2.(string) {
			return -1
		}
		return 1
	case int64:
		if d1.(int64) == d2.(int64) {
			return 0
		}
		if d1.(int64) < d2.(int64) {
			return -1
		}
		return 1
	case int32:
		if d1.(int32) == d2.(int32) {
			return 0
		}
		if d1.(int32) < d2.(int32) {
			return -1
		}
		return 1
	case float64:
		if d1.(float64) == d2.(float64) {
			return 0
		}
		if d1.(float64) < d2.(float64) {
			return -1
		}
		return 1
	case []byte:
		c := bytes.Compare(d1.([]byte), d2.([]byte))
		if c == 0 {
			return 0
		}
		if c < 0 {
			return -1
		}
		return 1
	case time.Time:
		if d1.(time.Time).Equal(d2.(time.Time)) {
			return 0
		}
		if d1.(time.Time).Before(d2.(time.Time)) {
			return -1
		}
		return 1
	case bool:
		if d1.(bool) == d2.(bool) {
			return 0
		}
		if d1.(bool) == false {
			return -1
		}
		return 1
	}
	panic(d1)
}

// CreateIfNotExists inserts a row if it isn't already there. The basic flow is:
// Find the partition, if it's not there, then create it and insert the row there
// If the partition is there, and there's data in it, and there's no clustering key, then fail
// Otherwise, search the partition for the exact same clustering keys. If there, fail
// if not, then insert it at the right spot (sort.Search does most of the heavy lifting here)
func (c *Connector) CreateIfNotExists(_ context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.data == nil {
		c.data = make(map[string]map[string][]map[string]dosa.FieldValue)
	}
	if c.data[ei.Def.Name] == nil {
		c.data[ei.Def.Name] = make(map[string][]map[string]dosa.FieldValue)
	}
	entityRef := c.data[ei.Def.Name]
	encodedPartitionKey := partitionKeyBuilder(ei, values)
	if entityRef[encodedPartitionKey] == nil {
		entityRef[encodedPartitionKey] = make([]map[string]dosa.FieldValue, 0, 1)
	}
	partitionRef := entityRef[encodedPartitionKey]
	// no data in this partition? easy out!
	if len(partitionRef) == 0 {
		entityRef[encodedPartitionKey] = append(entityRef[encodedPartitionKey], values)
		return nil
	}

	if len(ei.Def.ClusteringKeySet()) == 0 {
		// no clustering key, so the row must already exist
		return &dosa.ErrAlreadyExists{}
	}
	// there is a clustering key, find the insertion point (binary search would be fastest)
	found, offset := findInsertionPoint(ei, partitionRef, values)
	if found {
		return &dosa.ErrAlreadyExists{}
	}
	// perform slice magic to insert value at given offset
	l := len(entityRef[encodedPartitionKey])                                                                     // get length
	entityRef[encodedPartitionKey] = append(entityRef[encodedPartitionKey], entityRef[encodedPartitionKey][l-1]) // copy last element
	// scoot over remaining elements
	copy(entityRef[encodedPartitionKey][offset+1:], entityRef[encodedPartitionKey][offset:])
	// and plunk value into appropriate location
	entityRef[encodedPartitionKey][offset] = values

	return nil
}

// Read searches for a row. First, it finds the partition, then it searches in the partition for
// the data, and returns it when it finds it. Again, sort.Search does most of the heavy lifting
// within a partition
func (c *Connector) Read(_ context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.data[ei.Def.Name] == nil {
		return nil, &dosa.ErrNotFound{}
	}
	entityRef := c.data[ei.Def.Name]
	encodedPartitionKey := partitionKeyBuilder(ei, values)
	partitionRef := entityRef[encodedPartitionKey]
	// no data in this partition? easy out!
	if len(partitionRef) == 0 {
		return nil, &dosa.ErrNotFound{}
	}

	if len(ei.Def.ClusteringKeySet()) == 0 {
		return filterSet(partitionRef[0], fieldsToRead), nil
	}
	// clustering key, search for the value in the set
	found, inx := findInsertionPoint(ei, partitionRef, values)
	if !found {
		return nil, &dosa.ErrNotFound{}
	}
	return filterSet(partitionRef[inx], fieldsToRead), nil
}

// filterSet is a helper that creates a smaller map when the fieldsToRead has been
// specified
func filterSet(set map[string]dosa.FieldValue, fieldsToRead []string) map[string]dosa.FieldValue {
	if fieldsToRead == nil {
		return set
	}
	newSet := make(map[string]dosa.FieldValue)
	for _, field := range fieldsToRead {
		if val, ok := set[field]; ok {
			newSet[field] = val
		}
	}
	return newSet
}

// Upsert works a lot like CreateIfNotExists but merges the data when it finds an existing row
func (c *Connector) Upsert(_ context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.data == nil {
		c.data = make(map[string]map[string][]map[string]dosa.FieldValue)
	}
	if c.data[ei.Def.Name] == nil {
		c.data[ei.Def.Name] = make(map[string][]map[string]dosa.FieldValue)
	}
	entityRef := c.data[ei.Def.Name]
	encodedPartitionKey := partitionKeyBuilder(ei, values)
	if entityRef[encodedPartitionKey] == nil {
		entityRef[encodedPartitionKey] = make([]map[string]dosa.FieldValue, 0, 1)
	}
	partitionRef := entityRef[encodedPartitionKey]
	// no data in this partition? easy out!
	if len(partitionRef) == 0 {
		entityRef[encodedPartitionKey] = append(entityRef[encodedPartitionKey], values)
		return nil
	}

	if len(ei.Def.ClusteringKeySet()) == 0 {
		// no clustering key, so the row must already exist, merge it
		merge(partitionRef[0], values)
		return nil
	}
	// there is a clustering key, find the insertion point (binary search would be fastest)
	found, offset := findInsertionPoint(ei, partitionRef, values)
	if found {
		merge(partitionRef[offset], values)
		return nil
	}
	// perform slice magic to insert value at given offset
	l := len(entityRef[encodedPartitionKey])                                                                     // get length
	entityRef[encodedPartitionKey] = append(entityRef[encodedPartitionKey], entityRef[encodedPartitionKey][l-1]) // copy last element
	// scoot over remaining elements
	copy(entityRef[encodedPartitionKey][offset+1:], entityRef[encodedPartitionKey][offset:])
	// and plunk value into appropriate location
	entityRef[encodedPartitionKey][offset] = values
	return nil
}
func merge(into map[string]dosa.FieldValue, from map[string]dosa.FieldValue) {
	for k, v := range from {
		into[k] = v
	}
}

// Remove deletes a single row
func (c *Connector) Remove(_ context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.data[ei.Def.Name] == nil {
		return &dosa.ErrNotFound{}
	}
	entityRef := c.data[ei.Def.Name]
	encodedPartitionKey := partitionKeyBuilder(ei, values)
	if entityRef[encodedPartitionKey] == nil {
		return &dosa.ErrNotFound{}
	}
	partitionRef := entityRef[encodedPartitionKey]
	// no data in this partition? easy out!
	if len(partitionRef) == 0 {
		return &dosa.ErrNotFound{}
	}

	// no clustering keys? Simple, delete this
	if len(ei.Def.ClusteringKeySet()) == 0 {
		entityRef[encodedPartitionKey] = nil
		return nil
	}
	found, offset := findInsertionPoint(ei, partitionRef, values)
	if found {
		entityRef[encodedPartitionKey] = append(entityRef[encodedPartitionKey][:offset], entityRef[encodedPartitionKey][offset+1:]...)
		return nil
	}
	return &dosa.ErrNotFound{}
}

// Range returns a slice of data from the datastore
func (c *Connector) Range(_ context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.data[ei.Def.Name] == nil {
		return nil, "", &dosa.ErrNotFound{}
	}
	entityRef := c.data[ei.Def.Name]

	// find the equals conditions on each of the partition keys
	values := map[string]dosa.FieldValue{}
	for _, pk := range ei.Def.Key.PartitionKeys {
		// TODO: assert len(columnConditions[pk] == 1
		// TODO: assert columnConditions[pk][0].Op is equals
		values[pk] = columnConditions[pk][0].Value
	}

	encodedPartitionKey := partitionKeyBuilder(ei, values)
	partitionRef := entityRef[encodedPartitionKey]
	// no data in this partition? easy out!
	if len(partitionRef) == 0 {
		return nil, "", &dosa.ErrNotFound{}
	}
	// hunt through the partitionRef and return values that match search criteria
	// TODO: This can be done much faster using a binary search
	startinx, endinx := 0, len(partitionRef)-1
	for startinx < len(partitionRef) && !matchesClusteringConditions(ei, columnConditions, partitionRef[startinx]) {
		startinx++
	}
	// TODO: adjust startinx with a passed in token
	for endinx >= startinx && !matchesClusteringConditions(ei, columnConditions, partitionRef[endinx]) {
		endinx--

	}
	if endinx <= startinx {
		return nil, "", &dosa.ErrNotFound{}
	}
	// TODO: enforce limits and return a token when there are more rows
	// TODO: use filterSet() to remove columns not asked for
	return partitionRef[startinx : endinx+1], "", nil
}

// matchesClusteringConditions checks if a data row matches the conditions in the columnConditions that apply to
// clustering columns. If a condition does NOT match, it returns false, otherwise true
// This function is pretty fast if there are no conditions on the clustering columns
func matchesClusteringConditions(ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, data map[string]dosa.FieldValue) bool {
	for _, col := range ei.Def.Key.ClusteringKeys {
		if conds, ok := columnConditions[col.Name]; ok {
			// conditions exist on this clustering key
			for _, cond := range conds {
				if !passCol(data[col.Name], cond) {
					return false
				}
			}
		}
	}
	return true
}

// passCol checks if a column passes a specific condition
func passCol(data dosa.FieldValue, cond *dosa.Condition) bool {
	cmp := compareType(data, cond.Value)
	switch cond.Op {
	case dosa.Eq:
		return cmp == 0
	case dosa.Gt:
		return cmp > 0
	case dosa.GtOrEq:
		return cmp >= 0
	case dosa.Lt:
		return cmp < 0
	case dosa.LtOrEq:
		return cmp <= 0
	}
	panic("invalid operator " + cond.Op.String())
}

// Scan returns all the rows
func (c *Connector) Scan(_ context.Context, ei *dosa.EntityInfo, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.data[ei.Def.Name] == nil {
		return nil, "", &dosa.ErrNotFound{}
	}
	entityRef := c.data[ei.Def.Name]
	allTheThings := make([]map[string]dosa.FieldValue, 0)
	// TODO: stop when we reach the limit, and make a token for continuation
	for _, vals := range entityRef {
		allTheThings = append(allTheThings, vals...)
	}
	if len(allTheThings) == 0 {
		return nil, "", &dosa.ErrNotFound{}
	}
	return allTheThings, "", nil
}

// CheckSchema is just a stub; there is no schema management for the in memory connector
// since creating a new one leaves you with no data!
func (c *Connector) CheckSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) (int32, error) {
	return 1, nil
}

// Shutdown deletes all the data
func (c *Connector) Shutdown() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.data = nil
	return nil
}

func init() {
	dosa.RegisterConnector("memory", func(args map[string]interface{}) (dosa.Connector, error) {
		return &Connector{}, nil
	})
}
