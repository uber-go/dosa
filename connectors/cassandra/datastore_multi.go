package cassandra

//
//import (
//	"context"
//
//	"sync"
//
//	"sync/atomic"
//
//	"github.com/pkg/errors"
//	"github.com/uber-go/dosa"
//)
//
//func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, keys []map[string]dosa.FieldValue, fieldsToRead []string) ([]*dosa.FieldValuesOrError, error) {
//	stStore, err := d.getStStore(ei)
//	if err != nil {
//		return nil, errors.Wrap(err, "failed to multiread, cannot get backend store")
//	}
//
//	// stapi-go doesn't allow specifying complex IN condition such as (k1, k2) IN ((1,2), (3,4)
//	// And it's bad practice to use IN condition that involves multiple partitions anyway (pressure on coordinator node)
//	// https://lostechies.com/ryansvihla/2014/09/22/cassandra-query-patterns-not-using-the-in-query-for-multiple-partitions/
//	//
//	// So, we will just issue individual queries to C*, in parallel.
//
//	multiResult := make([]*dosa.FieldValuesOrError, len(keys))
//	sg := &semGroup{sem: newSemaphore(d.config.getMaxParallelMultiOps())}
//	for i, k := range keys {
//		sg.addOne()
//		go func(pos int, innerK map[string]dosa.FieldValue) {
//			defer sg.finishOne()
//			defer sg.recoverPanic()
//			res, err := doRead(ctx, stStore, ei, innerK, fieldsToRead)
//			if err != nil {
//				multiResult[pos] = &dosa.FieldValuesOrError{Error: err}
//			} else {
//				multiResult[pos] = &dosa.FieldValuesOrError{Values: res}
//			}
//		}(i, k)
//	}
//	if err := sg.wait(); err != nil {
//		return nil, errors.Wrap(err, "failed to multiread")
//	}
//
//	return multiResult, nil
//}
//
//func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
//	stStore, err := d.getStStore(ei)
//	if err != nil {
//		return nil, errors.Wrap(err, "failed to multiupsert, cannot get backend store")
//	}
//	multiResult := make([]error, len(multiValues))
//	sg := &semGroup{sem: newSemaphore(d.config.getMaxParallelMultiOps())}
//	for i, values := range multiValues {
//		sg.addOne()
//		go func(pos int, innerValues map[string]dosa.FieldValue) {
//			defer sg.finishOne()
//			defer sg.recoverPanic()
//			multiResult[pos] = doUpsert(ctx, stStore, ei, innerValues)
//		}(i, values)
//	}
//	if err := sg.wait(); err != nil {
//		return nil, errors.Wrap(err, "failed to multiupsert")
//	}
//	return multiResult, nil
//}
//
//func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiKeys []map[string]dosa.FieldValue) ([]error, error) {
//	stStore, err := d.getStStore(ei)
//	if err != nil {
//		return nil, errors.Wrap(err, "failed to multiremove, cannot get backend store")
//	}
//	multiResult := make([]error, len(multiKeys))
//	sg := &semGroup{sem: newSemaphore(d.config.getMaxParallelMultiOps())}
//	for i, keys := range multiKeys {
//		sg.addOne()
//		go func(pos int, innerKeys map[string]dosa.FieldValue) {
//			defer sg.finishOne()
//			defer sg.recoverPanic()
//			multiResult[pos] = doRemove(ctx, stStore, ei, innerKeys)
//		}(i, keys)
//	}
//	if err := sg.wait(); err != nil {
//		return nil, errors.Wrap(err, "failed to multiremove")
//	}
//
//	return multiResult, nil
//}
//
//// semGroup is a convenience struct to hold a semaphore and a waitgroup.
//// It also helps handling panics in spawned goroutines.
//type semGroup struct {
//	sem  semaphore
//	wg   sync.WaitGroup
//	errV atomic.Value
//}
//
//// addOne adds one work item after acquiring semaphore
//func (s *semGroup) addOne() {
//	s.sem.acquire()
//	s.wg.Add(1)
//}
//
//// finishOne finishes one work item and releases semaphore.
//func (s *semGroup) finishOne() {
//	s.wg.Done()
//	s.sem.release()
//}
//
//// catch panic in spawned goroutines so that our service process won't die
//func (s *semGroup) recoverPanic() {
//	if r := recover(); r != nil {
//		s.errV.Store(errors.Errorf("gorouting paniced: %v", r))
//	}
//}
//
//// wait waits for for all working items to finish
//// It returns error if any goroutine paniced and was recovered in recoverPanic
//func (s *semGroup) wait() error {
//	s.wg.Wait()
//	if s.errV.Load() != nil {
//		return s.errV.Load().(error)
//	}
//	return nil
//}
//
//type semaphore chan struct{}
//
//func newSemaphore(n int) semaphore {
//	s := make(chan struct{}, n)
//	for i := 0; i < n; i++ {
//		s <- struct{}{}
//	}
//	return s
//}
//
//func (s semaphore) acquire() {
//	<-s
//}
//
//func (s semaphore) release() {
//	s <- struct{}{}
//}
