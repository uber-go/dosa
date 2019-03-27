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
	"fmt"
	"time"
)

// PrefixMetadata is metadata about a prefix in production (note: we do not track prefixes for dev scopes).
type PrefixMetadata struct {
	Entity      `dosa:"primaryKey=(Name)" json:"-"`
	Name        string     `json:"name"`
	Owner       string     `json:"owner"` // Owning group name (or the same as Creator)
	Flags       int64      `json:"flags"`
	Version     int32      `json:"version"`
	EntityStr   string     `json:"entity_str,omitempty"` // With ":" separators
	Cluster     string     `json:"cluster,omitempty"`    // Host DB cluster
	Creator     string     `json:"creator"`
	CreatedOn   time.Time  `json:"created_on"`
	ExpiresOn   *time.Time `json:"expires_on,omitempty"`
	ExtendCount int32      `json:"extend_count,omitempty"`
	NotifyCount int32      `json:"notify_count,omitempty"`
	ReadMaxRPS  int32      `json:"read_max_rps"`
	WriteMaxRPS int32      `json:"write_max_rps"`

	// This is for convenience only, not stored in the DB:
	Entities StringSet `dosa:"-" json:"-"`
}

func (md *PrefixMetadata) String() string {
	s := fmt.Sprintf("<Scope %s (%s): owner=%s, creator=%s, created=%v", md.Name, Production,
		md.Owner, md.Creator, md.CreatedOn)
	if md.ExpiresOn != nil {
		s += fmt.Sprintf(", expires=%v", *md.ExpiresOn)
	}
	if len(md.Entities) > 0 {
		s += fmt.Sprintf(", entities=%s", md.Entities)
	}
	if len(md.Cluster) > 0 {
		s += fmt.Sprintf(", cluster=%v", md.Cluster)
	}
	if md.ExtendCount > 0 {
		s += fmt.Sprintf(", extended=%d", md.ExtendCount)
	}
	if md.NotifyCount > 0 {
		s += fmt.Sprintf(", notified=%d", md.ExtendCount)
	}
	return s + ">"
}

// PrefixFlagType is a set of prefix flags
type PrefixFlagType int64

// G E T T E R S

// GetName is the name of the prefix.
func (md *PrefixMetadata) GetName() string {
	return md.Name
}

// GetOwner is the owner of the prefix.
func (md *PrefixMetadata) GetOwner() string {
	return md.Owner
}

// GetType is the type of the prefix.
func (md *PrefixMetadata) GetType() int32 {
	return int32(Production)
}

// GetFlags is the flags of the prefix.
func (md *PrefixMetadata) GetFlags() int64 {
	return md.Flags
}

// GetVersion is the version of the prefix.
func (md *PrefixMetadata) GetVersion() int32 {
	return md.Version
}

// GetEntityStr is the encoded list of all prefixes.
func (md *PrefixMetadata) GetEntityStr() string {
	return md.EntityStr
}

// GetCluster is the cluster name of the prefix.
func (md *PrefixMetadata) GetCluster() string {
	return md.Cluster
}

// GetCreator is the creator of the prefix.
func (md *PrefixMetadata) GetCreator() string {
	return md.Creator
}

// GetCreatedOn is the create timestamp of the prefix.
func (md *PrefixMetadata) GetCreatedOn() time.Time {
	return md.CreatedOn
}

// GetExpiresOn is the expiration timestamp of the prefix.
func (md *PrefixMetadata) GetExpiresOn() *time.Time {
	if md.ExpiresOn == nil {
		return nil
	}
	rv := *md.ExpiresOn
	return &rv
}

// GetExtendCount is the extend count of the prefix.
func (md *PrefixMetadata) GetExtendCount() int32 {
	return md.ExtendCount
}

// GetNotifyCount is the notify count of the prefix.
func (md *PrefixMetadata) GetNotifyCount() int32 {
	return md.NotifyCount
}

// GetReadMaxRPS is the max read rate for the prefix.
func (md *PrefixMetadata) GetReadMaxRPS() int32 {
	return md.ReadMaxRPS
}

// GetWriteMaxRPS is the max write rate for the prefix.
func (md *PrefixMetadata) GetWriteMaxRPS() int32 {
	return md.WriteMaxRPS
}

// S E T T E R S

// SetName sets the name of the prefix.
func (md *PrefixMetadata) SetName(x string) {
	md.Name = x
}

// SetOwner sets the owner of the prefix.
func (md *PrefixMetadata) SetOwner(x string) {
	md.Owner = x
}

// SetType sets the type of the prefix.
func (md *PrefixMetadata) SetType(x int32) {
	if ScopeType(x) != Production {
		panic(fmt.Errorf("attempt to set prefix type to %s", ScopeType(x)))
	}
}

// SetFlags sets the flags of the prefix.
func (md *PrefixMetadata) SetFlags(x int64) {
	md.Flags = x
}

// SetVersion sets the version of the prefix.
func (md *PrefixMetadata) SetVersion(x int32) {
	md.Version = x
}

// SetEntityStr sets the encoded list of all entities.
func (md *PrefixMetadata) SetEntityStr(x string) {
	md.EntityStr = x
}

// SetCluster sets the cluster name of the prefix.
func (md *PrefixMetadata) SetCluster(x string) {
	md.Cluster = x
}

// SetCreator sets the creator of the prefix.
func (md *PrefixMetadata) SetCreator(x string) {
	md.Creator = x
}

// SetCreatedOn sets the create timestamp of the prefix.
func (md *PrefixMetadata) SetCreatedOn(x time.Time) {
	md.CreatedOn = x
}

// SetExpiresOn sets the expiration timestamp of the prefix.
func (md *PrefixMetadata) SetExpiresOn(x time.Time) {
	md.ExpiresOn = &x
}

// SetExtendCount sets the extend count of the prefix.
func (md *PrefixMetadata) SetExtendCount(x int32) {
	md.ExtendCount = x
}

// SetNotifyCount sets the notify count of the prefix.
func (md *PrefixMetadata) SetNotifyCount(x int32) {
	md.NotifyCount = x
}

// SetReadMaxRPS sets the max read rate for the prefix.
func (md *PrefixMetadata) SetReadMaxRPS(x int32) {
	md.ReadMaxRPS = x
}

// SetWriteMaxRPS sets the max write rate for the prefix.
func (md *PrefixMetadata) SetWriteMaxRPS(x int32) {
	md.WriteMaxRPS = x
}
