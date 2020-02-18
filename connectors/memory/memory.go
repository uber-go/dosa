// Copyright (c) 2019 Uber Technologies, Inc.
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
	"encoding/base64"
	"sort"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
	"github.com/uber-go/dosa/encoding"
)

// Connector is an in-memory connector.
// The in-memory connector stores its data like this:
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
//
// NOTE: The memory connector doesn't support TTL. All the data is stored in memory until a manual delete.
type Connector struct {
	base.Connector
	data map[string]map[string][]map[string]dosa.FieldValue
	lock sync.RWMutex
}

// partitionRange represents one section of a partition.
type partitionRange struct {
	entityRef    map[string][]map[string]dosa.FieldValue
	partitionKey string
	start        int
	end          int
}

const defaultRangeLimit = 200

// remove deletes the values referenced by the partitionRange. Since this function modifies
// the data stored in the in-memory connector, a write lock must be held when calling
// this function.
//
// Note this function can't be called more than once. Calling it more than once will cause a panic.
func (pr *partitionRange) remove() {
	partitionRef := pr.entityRef[pr.partitionKey]
	pr.entityRef[pr.partitionKey] = append(partitionRef[:pr.start], partitionRef[pr.end+1:]...)
	pr.entityRef = nil
	pr.partitionKey = ""
	pr.start = 0
	pr.end = 0
}

// values returns all the values in the partition range
func (pr *partitionRange) values() []map[string]dosa.FieldValue {
	return pr.entityRef[pr.partitionKey][pr.start : pr.end+1]
}

// partitionKeyBuilder extracts the partition key components from the map and encodes them,
// generating a unique string. It uses the encoding/gob method to make a byte array as the
// key, and returns this as a string
func partitionKeyBuilder(pk *dosa.PrimaryKey, values map[string]dosa.FieldValue) (string, error) {
	encoder := encoding.NewGobEncoder()
	var encodedKey []byte
	for _, k := range pk.PartitionKeys {
		if v, ok := values[k]; ok {
			encodedVal, _ := encoder.Encode(v)
			encodedKey = append(encodedKey, encodedVal...)
		} else {
			return "", errors.Errorf("Missing value for partition key %q", k)
		}
	}
	return string(encodedKey), nil
}

// findInsertionPoint locates the place within a partition where the data belongs.
// It inspects the clustering key values found in the insertMe value and figures out
// where they go in the data slice. It doesn't change anything, but it does let you
// know if it found an exact match or if it's just not there. When it's not there,
// it indicates where it is supposed to get inserted
func findInsertionPoint(pk *dosa.PrimaryKey, data []map[string]dosa.FieldValue, insertMe map[string]dosa.FieldValue) (found bool, idx int) {
	found = false
	idx = sort.Search(len(data), func(offset int) bool {
		cmp := compareRows(pk, data[offset], insertMe)
		if cmp == 0 {
			found = true
		}
		return cmp >= 0
	})
	return
}

// compareRows compares two maps of row data based on clustering keys.
func compareRows(pk *dosa.PrimaryKey, v1 map[string]dosa.FieldValue, v2 map[string]dosa.FieldValue) (cmp int8) {
	keys := pk.ClusteringKeys
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

// copyRows copies all the rows in the given slice and returns a new slice with copies
// of each row. Order is maintained.
func copyRows(rows []map[string]dosa.FieldValue) []map[string]dosa.FieldValue {
	copied := make([]map[string]dosa.FieldValue, len(rows))
	for i, row := range rows {
		copied[i] = copyRow(row)
	}
	return copied
}

// copyRow takes in a given "row" and returns a new map containing all of the same
// values that were in the given row.
func copyRow(row map[string]dosa.FieldValue) map[string]dosa.FieldValue {
	copied := make(map[string]dosa.FieldValue, len(row))
	for k, v := range row {
		copied[k] = v
	}
	return copied
}

// compareType compares a single DOSA field based on the type. This code assumes the types of each
// of the columns are the same, or it will panic
func compareType(d1 dosa.FieldValue, d2 dosa.FieldValue) int8 {
	switch d1 := d1.(type) {
	case dosa.UUID:
		u1 := uuid.FromStringOrNil(string(d1))
		u2 := uuid.FromStringOrNil(string(d2.(dosa.UUID)))
		if u1.Version() != u2.Version() {
			if u1.Version() < u2.Version() {
				return -1
			}
			return 1
		}
		if u1.Version() == 1 {
			// compare time UUIDs
			t1, _ := uuid.TimestampFromV1(u1)
			t2, _ := uuid.TimestampFromV1(u2)
			if t1 == t2 {
				return 0
			}
			if t1 < t2 {
				return -1
			}
			return 1
		}

		// version
		if string(d1) == string(d2.(dosa.UUID)) {
			return 0
		}
		if string(d1) < string(d2.(dosa.UUID)) {
			return -1
		}
		return 1
	case string:
		if d1 == d2.(string) {
			return 0
		}
		if d1 < d2.(string) {
			return -1
		}
		return 1
	case int64:
		if d1 == d2.(int64) {
			return 0
		}
		if d1 < d2.(int64) {
			return -1
		}
		return 1
	case int32:
		if d1 == d2.(int32) {
			return 0
		}
		if d1 < d2.(int32) {
			return -1
		}
		return 1
	case float64:
		if d1 == d2.(float64) {
			return 0
		}
		if d1 < d2.(float64) {
			return -1
		}
		return 1
	case []byte:
		c := bytes.Compare(d1, d2.([]byte))
		if c == 0 {
			return 0
		}
		if c < 0 {
			return -1
		}
		return 1
	case time.Time:
		if d1.Equal(d2.(time.Time)) {
			return 0
		}
		if d1.Before(d2.(time.Time)) {
			return -1
		}
		return 1
	case bool:
		if d1 == d2.(bool) {
			return 0
		}
		if d1 == false {
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

	valsCopy := copyRow(values)
	_, err := c.mergedInsert(ei.Def.Name, ei.Def.Key, valsCopy, func(into map[string]dosa.FieldValue, from map[string]dosa.FieldValue) error {
		return &dosa.ErrAlreadyExists{}
	}, false)
	if err != nil {
		return err
	}
	for iName, iDef := range ei.Def.Indexes {
		// this error must be ignored, so we skip indexes when the value
		// for one of the index fields is not specified
		_, _ = c.mergedInsert(iName, ei.Def.UniqueKey(iDef.Key), valsCopy, overwriteValuesFunc, false)
	}
	return nil
}

// Read searches for a row. First, it finds the partition, then it searches in the partition for
// the data, and returns it when it finds it. Again, sort.Search does most of the heavy lifting
// within a partition
func (c *Connector) Read(_ context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, minimumFields []string) (map[string]dosa.FieldValue, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	entityRef := c.data[ei.Def.Name]
	encodedPartitionKey, err := partitionKeyBuilder(ei.Def.Key, values)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot build partition key for entity %q", ei.Def.Name)
	}
	if c.data[ei.Def.Name] == nil {
		return nil, &dosa.ErrNotFound{}
	}
	partitionRef := entityRef[encodedPartitionKey]
	// no data in this partition? easy out!
	if len(partitionRef) == 0 {
		return nil, &dosa.ErrNotFound{}
	}

	if len(ei.Def.Key.ClusteringKeySet()) == 0 {
		return copyRow(partitionRef[0]), nil
	}
	// clustering key, search for the value in the set
	found, inx := findInsertionPoint(ei.Def.Key, partitionRef, values)
	if !found {
		return nil, &dosa.ErrNotFound{}
	}
	return copyRow(partitionRef[inx]), nil
}

// MultiRead fetches a series of values at once.
func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue, minimumFields []string) ([]*dosa.FieldValuesOrError, error) {
	var fvoes []*dosa.FieldValuesOrError
	for _, v := range values {
		fieldValue, err := c.Read(ctx, ei, v, minimumFields)
		fvoe := &dosa.FieldValuesOrError{}
		if err != nil {
			fvoe.Error = err
		} else {
			fvoe.Values = fieldValue
		}

		fvoes = append(fvoes, fvoe)
	}

	return fvoes, nil
}

// MultiUpsert upserts a series of values at once.
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue) ([]error, error) {
	// Note we do not lock here. This is representative of the behavior one would see in a deployed environment
	var errs []error
	for _, v := range values {
		errs = append(errs, c.Upsert(ctx, ei, v))
	}

	return errs, nil
}

// MultiRemove removes a series of values at once.
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	// Note we do not lock here. This is representative of the behavior one would see in a deployed environment
	var errs []error
	for _, v := range multiValues {
		errs = append(errs, c.Remove(ctx, ei, v))
	}

	return errs, nil
}

func overwriteValuesFunc(into map[string]dosa.FieldValue, from map[string]dosa.FieldValue) error {
	for k, v := range from {
		into[k] = v
	}
	return nil
}

// Upsert works a lot like CreateIfNotExists but merges the data when it finds an existing row
func (c *Connector) Upsert(_ context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	valsCopy := copyRow(values)
	var oldValues map[string]dosa.FieldValue
	var err error
	if oldValues, err = c.mergedInsert(ei.Def.Name, ei.Def.Key, valsCopy, overwriteValuesFunc, true); err != nil {
		return err
	}
	for iName, iDef := range ei.Def.Indexes {
		if oldValues != nil {
			c.removeItem(iName, ei.Def.UniqueKey(iDef.Key), oldValues)
		}
		_, _ = c.mergedInsert(iName, ei.Def.UniqueKey(iDef.Key), valsCopy, overwriteValuesFunc, false)
	}

	return nil
}

func (c *Connector) mergedInsert(name string,
	pk *dosa.PrimaryKey,
	values map[string]dosa.FieldValue,
	mergeFunc func(map[string]dosa.FieldValue, map[string]dosa.FieldValue) error,
	returnCopy bool) (map[string]dosa.FieldValue, error) {

	if c.data[name] == nil {
		c.data[name] = make(map[string][]map[string]dosa.FieldValue)
	}
	entityRef := c.data[name]
	encodedPartitionKey, err := partitionKeyBuilder(pk, values)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot build partition key for %q", name)
	}
	if entityRef[encodedPartitionKey] == nil {
		entityRef[encodedPartitionKey] = make([]map[string]dosa.FieldValue, 0, 1)
	}
	partitionRef := entityRef[encodedPartitionKey]
	// no data in this partition? easy out!
	if len(partitionRef) == 0 {
		entityRef[encodedPartitionKey] = append(entityRef[encodedPartitionKey], values)
		return nil, nil
	}

	if len(pk.ClusteringKeySet()) == 0 {
		// no clustering key, so the row must already exist, merge it
		if returnCopy {
			return copyRow(partitionRef[0]), mergeFunc(partitionRef[0], values)
		}
		return nil, mergeFunc(partitionRef[0], values)
	}
	// there is a clustering key, find the insertion point (binary search would be fastest)
	found, offset := findInsertionPoint(pk, partitionRef, values)
	if found {
		if returnCopy {
			return copyRow(partitionRef[offset]), mergeFunc(partitionRef[offset], values)
		}
		return nil, mergeFunc(partitionRef[offset], values)
	}
	// perform slice magic to insert value at given offset
	l := len(entityRef[encodedPartitionKey])                                                                     // get length
	entityRef[encodedPartitionKey] = append(entityRef[encodedPartitionKey], entityRef[encodedPartitionKey][l-1]) // copy last element
	// scoot over remaining elements
	copy(entityRef[encodedPartitionKey][offset+1:], entityRef[encodedPartitionKey][offset:])
	// and plunk value into appropriate location
	entityRef[encodedPartitionKey][offset] = values
	return nil, nil
}

// Remove deletes a single row
// There's no way to return an error from this method
func (c *Connector) Remove(_ context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.data[ei.Def.Name] == nil {
		return nil
	}
	removedValues := c.removeItem(ei.Def.Name, ei.Def.Key, values)
	if removedValues != nil {
		for iName, iDef := range ei.Def.Indexes {
			c.removeItem(iName, ei.Def.UniqueKey(iDef.Key), removedValues)
		}
	}
	return nil
}

func (c *Connector) removeItem(name string, key *dosa.PrimaryKey, values map[string]dosa.FieldValue) map[string]dosa.FieldValue {
	entityRef := c.data[name]
	encodedPartitionKey, err := partitionKeyBuilder(key, values)
	if err != nil || entityRef[encodedPartitionKey] == nil {
		return nil
	}
	partitionRef := entityRef[encodedPartitionKey]
	// no data in this partition? easy out!
	if len(partitionRef) == 0 {
		return nil
	}

	// no clustering keys? Simple, delete this
	if len(key.ClusteringKeySet()) == 0 {
		// NOT delete(entityRef, encodedPartitionKey)
		// Unfortunately, Scan relies on the fact that these are not completely deleted
		deletedValues := entityRef[encodedPartitionKey][0]
		entityRef[encodedPartitionKey] = nil
		return deletedValues
	}

	var deletedValues map[string]dosa.FieldValue
	found, offset := findInsertionPoint(key, partitionRef, values)
	if found {
		deletedValues = entityRef[encodedPartitionKey][offset]
		entityRef[encodedPartitionKey] = append(entityRef[encodedPartitionKey][:offset], entityRef[encodedPartitionKey][offset+1:]...)
	}
	return deletedValues
}

// RemoveRange removes all of the elements in the range specified by the entity info and the column conditions.
func (c *Connector) RemoveRange(_ context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	partitionRange, _, err := c.findRange(ei, columnConditions, false)
	if err != nil {
		return err
	}
	if partitionRange != nil {
		for iName, iDef := range ei.Def.Indexes {
			for _, vals := range partitionRange.values() {
				c.removeItem(iName, ei.Def.UniqueKey(iDef.Key), vals)
			}
		}
		partitionRange.remove()
	}

	return nil
}

// Range returns a slice of data from the datastore
func (c *Connector) Range(_ context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	partitionRange, key, err := c.findRange(ei, columnConditions, true)
	if err != nil {
		return nil, "", errors.Wrap(err, "Invalid range conditions")
	}
	if partitionRange == nil {
		return []map[string]dosa.FieldValue{}, "", nil
	}

	if token != "" {
		// if we have a token, use it to determine the offset to start from
		values, err := decodeToken(token)
		if err != nil {
			return nil, "", errors.Wrapf(err, "Invalid token %q", token)
		}
		found, offset := findInsertionPoint(key, partitionRange.values(), values)
		if found {
			partitionRange.start += offset + 1
		} else {
			partitionRange.start += offset
		}
	}

	if limit == dosa.AdaptiveRangeLimit {
		limit = defaultRangeLimit
	}

	slice := partitionRange.values()
	token = ""
	if len(slice) > limit {
		token = makeToken(slice[limit-1])
		slice = slice[:limit]
	}

	return copyRows(slice), token, nil
}

func makeToken(v map[string]dosa.FieldValue) string {
	encoder := encoding.NewGobEncoder()
	encodedKey, err := encoder.Encode(v)
	if err != nil {
		// this should really be impossible, unless someone forgot to
		// register some newly supported type with the encoder
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(encodedKey)
}

func decodeToken(token string) (values map[string]dosa.FieldValue, err error) {
	gobData, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}

	decoder := encoding.NewGobEncoder()
	err = decoder.Decode(gobData, &values)
	return values, err
}

// findRange finds the partitionRange specified by the given entity info and column conditions.
// In the case that no entities are found an empty partitionRange with a nil partition field will be returned.
//
// Note that this function reads from the connector's data map. Any calling functions should hold
// at least a read lock on the map.
func (c *Connector) findRange(ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, searchIndexes bool) (*partitionRange, *dosa.PrimaryKey, error) {
	// no data at all, fine
	if c.data[ei.Def.Name] == nil {
		return nil, nil, nil
	}

	// find the equals conditions on each of the partition keys
	values := make(map[string]dosa.FieldValue)

	// figure out which "table" or "index" to use based on the supplied conditions
	name, key, err := ei.IndexFromConditions(columnConditions, searchIndexes)
	if err != nil {
		return nil, nil, err
	}

	for _, pk := range key.PartitionKeys {
		values[pk] = columnConditions[pk][0].Value
	}

	entityRef := c.data[name]
	// an error is impossible here, since the partition keys must be set from IndexFromConditions
	encodedPartitionKey, _ := partitionKeyBuilder(key, values)
	partitionRef := entityRef[encodedPartitionKey]
	// no data in this partition? easy out!
	if len(partitionRef) == 0 {
		return nil, nil, nil
	}
	// hunt through the partitionRef and return values that match search criteria
	// TODO: This can be done much faster using a binary search
	startinx, endinx := 0, len(partitionRef)-1
	for startinx < len(partitionRef) && !matchesClusteringConditions(key, columnConditions, partitionRef[startinx]) {
		startinx++
	}
	for endinx >= startinx && !matchesClusteringConditions(key, columnConditions, partitionRef[endinx]) {
		endinx--
	}
	if endinx < startinx {
		return nil, nil, nil
	}

	return &partitionRange{
		entityRef:    entityRef,
		partitionKey: encodedPartitionKey,
		start:        startinx,
		end:          endinx,
	}, key, nil
}

// matchesClusteringConditions checks if a data row matches the conditions in the columnConditions that apply to
// clustering columns. If a condition does NOT match, it returns false, otherwise true
// This function is pretty fast if there are no conditions on the clustering columns
func matchesClusteringConditions(key *dosa.PrimaryKey, columnConditions map[string][]*dosa.Condition, data map[string]dosa.FieldValue) bool {
	for _, col := range key.ClusteringKeys {
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
func (c *Connector) Scan(_ context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.data[ei.Def.Name] == nil {
		return []map[string]dosa.FieldValue{}, "", nil

	}
	entityRef := c.data[ei.Def.Name]
	allTheThings := make([]map[string]dosa.FieldValue, 0)

	// in order for Scan to be deterministic and continuable, we have
	// to sort the primary key references
	keys := make([]string, 0, len(entityRef))
	for key := range entityRef {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// if there was a token, decode it so we can determine the starting
	// partition key
	startPartKey, start, err := getStartingPoint(ei, token)
	if err != nil {
		return nil, "", errors.Wrapf(err, "Invalid token %s", token)
	}

	for _, key := range keys {
		if startPartKey != "" {
			// had a token, so we need to either partially skip or fully skip
			// depending on whether we found the token's partition key yet
			if key == startPartKey {
				// we reached the starting partition key, so stop skipping
				// future values, and add a portion of this one to the set
				startPartKey = ""
				found, offset := findInsertionPoint(ei.Def.Key, entityRef[key], start)
				if found {
					offset++
				}
				allTheThings = append(allTheThings, entityRef[key][offset:]...)
			} // else keep looking for this partition key
			continue
		}
		allTheThings = append(allTheThings, entityRef[key]...)
	}
	if len(allTheThings) == 0 {
		return []map[string]dosa.FieldValue{}, "", nil
	}
	// see if we need a token to return
	token = ""
	if len(allTheThings) > limit {
		token = makeToken(allTheThings[limit-1])
		allTheThings = allTheThings[:limit]
	}
	return copyRows(allTheThings), token, nil
}

// getStartingPoint determines the partition key of the starting point to resume a scan
// when a token is provided
func getStartingPoint(ei *dosa.EntityInfo, token string) (start string, startPartKey map[string]dosa.FieldValue, err error) {
	if token == "" {
		return "", map[string]dosa.FieldValue{}, nil
	}
	startPartKey, err = decodeToken(token)
	if err != nil {
		return "", map[string]dosa.FieldValue{}, errors.Wrapf(err, "Invalid token %q", token)
	}
	start, err = partitionKeyBuilder(ei.Def.Key, startPartKey)
	if err != nil {
		return "", map[string]dosa.FieldValue{}, errors.Wrapf(err, "Can't build partition key for %q", ei.Def.Name)
	}
	return start, startPartKey, nil
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

// NewConnector creates a new in-memory connector
func NewConnector() *Connector {
	c := Connector{}
	c.data = make(map[string]map[string][]map[string]dosa.FieldValue)
	return &c
}
