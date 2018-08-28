// Copyright (c) 2018 Uber Technologies, Inc.
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

package dosa

import (
	"sort"
	"strings"
	"time"
)

// Metadata objects are abstracted by an interface that defines getters and setters. Getters are
// named Get*() because we can't have fields and methods with the same name (and all fields must
// be exported in a DOSA entity).

// Metadata is an interface abstracting both ScopeMetadata and PrefixMetadata.
type Metadata interface {
	// GetName is the name of the object.
	GetName() string
	// GetOwner is the owner of the object.
	GetOwner() string
	// GetType is the type of the object.
	GetType() int32
	// GetFlags is the flags of the object.
	GetFlags() int64
	// GetVersion is the version of the object.
	GetVersion() int32
	// GetCluster is the cluster name of the object.
	GetCluster() string
	// GetCreator is the creator of the object.
	GetCreator() string
	// GetCreatedOn is the create timestamp of the object.
	GetCreatedOn() time.Time
	// GetExpiresOn is the expiration timestamp of the object.
	GetExpiresOn() time.Time
	// GetExtendCount is the extend count of the object.
	GetExtendCount() int32
	// GetNotifyCount is the notify count of the object.
	GetNotifyCount() int32
	// GetReadMaxRPS is the max read rate for the object.
	GetReadMaxRPS() int32
	// GetWriteMaxRPS is the max write rate for the object.
	GetWriteMaxRPS() int32

	// SetName sets the name of the object.
	SetName(string)
	// SetOwner sets the owner of the object.
	SetOwner(string)
	// SetType sets the type of the object.
	SetType(int32)
	// SetFlags sets the flags of the object.
	SetFlags(int64)
	// SetVersion sets the version of the object.
	SetVersion(int32)
	// SetCluster sets the cluster name of the object.
	SetCluster(string)
	// SetCreator sets the creator of the object.
	SetCreator(string)
	// SetCreatedOn sets the create timestamp of the object.
	SetCreatedOn(time.Time)
	// SetExpiresOn sets the expiration timestamp of the object.
	SetExpiresOn(time.Time)
	// SetExtendCount sets the extend count of the object.
	SetExtendCount(int32)
	// SetNotifyCount sets the notify count of the object.
	SetNotifyCount(int32)
	// SetReadMaxRPS sets the max read rate for the object.
	SetReadMaxRPS(int32)
	// SetWriteMaxRPS sets the max write rate for the object.
	SetWriteMaxRPS(int32)
}

// StringSet is a set of strings.
type StringSet map[string]struct{}

func (s StringSet) String() string {
	ss := make([]string, len(s))
	i := 0
	for k := range s {
		ss[i] = k
		i++
	}
	sort.Strings(ss)
	return "{" + strings.Join(ss, ", ") + "}"
}
