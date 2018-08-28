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
	"strings"
	"time"
)

// ScopeMetadata is metadata about a scope. (JSON tags to support MD setting CLI tools.)
type ScopeMetadata struct {
	Entity      `dosa:"primaryKey=(Name)" json:"-"`
	Name        string     `json:"name"`
	Owner       string     `json:"owner"` // Owning group name (or the same as Creator)
	Type        int32      `json:"type"`  // Production, Staging, or Development
	Flags       int64      `json:"flags"`
	Version     int32      `json:"version"`
	PrefixStr   string     `json:"prefix_str,omitempty"` // With ":" separators
	Cluster     string     `json:"cluster,omitempty"`    // Host DB cluster
	Creator     string     `json:"creator"`
	CreatedOn   time.Time  `json:"created_on"`
	ExpiresOn   *time.Time `json:"expires_on,omitempty"`
	ExtendCount int32      `json:"extend_count,omitempty"`
	NotifyCount int32      `json:"notify_count,omitempty"`
	ReadMaxRPS  int32      `json:"read_max_rps"`
	WriteMaxRPS int32      `json:"write_max_rps"`

	// This is for convenience only, not stored in the DB:
	Prefixes StringSet `dosa:"-" json:"-"`
}

// MetadataSchemaVersion is the version of the schema of the scope metadata
const MetadataSchemaVersion = 1

func (md *ScopeMetadata) String() string {
	s := fmt.Sprintf("<Scope %s (%s): owner=%s, creator=%s, created=%v", md.Name, ScopeType(md.Type),
		md.Owner, md.Creator, md.CreatedOn)
	if md.ExpiresOn != nil {
		s += fmt.Sprintf(", expires=%v", *md.ExpiresOn)
	}
	if len(md.Prefixes) > 0 {
		s += fmt.Sprintf(", prefixes=%s", md.Prefixes)
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

// ScopeType is the type of a scope
type ScopeType int32

// Scope types
const (
	// Development scope (also the default, so should be 0)
	Development ScopeType = iota
	// Staging doesn't really exist yet, but may in the future
	Staging
	// Production scope
	Production
)

func (t ScopeType) String() string {
	switch t {
	case Production:
		return "production"
	case Staging:
		return "staging"
	case Development:
		return "development"
	}
	return fmt.Sprintf("unknown scope type %d", t)
}

// ScopeFlagType is a set of scope flags
type ScopeFlagType int64

// Scope flags (remember to update ScopeFlagType.String)
const (
	// AccessFromProd means access is allowed from production (only relevant for Dev scopes)
	AccessFromProd ScopeFlagType = 1 << iota
)

func (f ScopeFlagType) String() string {
	fs := make([]string, 1)
	if f&AccessFromProd != 0 {
		fs = append(fs, "AccessFromProd")
	}
	return "{" + strings.Join(fs, ", ") + "}"
}

// G E T T E R S

// GetName is the name of the scope.
func (md *ScopeMetadata) GetName() string {
	return md.Name
}

// GetOwner is the owner of the scope.
func (md *ScopeMetadata) GetOwner() string {
	return md.Owner
}

// GetType is the type of the scope.
func (md *ScopeMetadata) GetType() int32 {
	return md.Type
}

// GetFlags is the flags of the scope.
func (md *ScopeMetadata) GetFlags() int64 {
	return md.Flags
}

// GetVersion is the version of the scope.
func (md *ScopeMetadata) GetVersion() int32 {
	return md.Version
}

// GetPrefixStr is the encoded list of all prefixes.
func (md *ScopeMetadata) GetPrefixStr() string {
	return md.PrefixStr
}

// GetCluster is the cluster name of the scope.
func (md *ScopeMetadata) GetCluster() string {
	return md.Cluster
}

// GetCreator is the creator of the scope.
func (md *ScopeMetadata) GetCreator() string {
	return md.Creator
}

// GetCreatedOn is the create timestamp of the scope.
func (md *ScopeMetadata) GetCreatedOn() time.Time {
	return md.CreatedOn
}

// GetExpiresOn is the expiration timestamp of the scope.
func (md *ScopeMetadata) GetExpiresOn() *time.Time {
	if md.ExpiresOn == nil {
		return nil
	}
	rv := *md.ExpiresOn
	return &rv
}

// GetExtendCount is the extend count of the scope.
func (md *ScopeMetadata) GetExtendCount() int32 {
	return md.ExtendCount
}

// GetNotifyCount is the notify count of the scope.
func (md *ScopeMetadata) GetNotifyCount() int32 {
	return md.NotifyCount
}

// GetReadMaxRPS is the max read rate for the scope.
func (md *ScopeMetadata) GetReadMaxRPS() int32 {
	return md.ReadMaxRPS
}

// GetWriteMaxRPS is the max write rate for the scope.
func (md *ScopeMetadata) GetWriteMaxRPS() int32 {
	return md.WriteMaxRPS
}

// S E T T E R S

// SetName sets the name of the scope.
func (md *ScopeMetadata) SetName(x string) {
	md.Name = x
}

// SetOwner sets the owner of the scope.
func (md *ScopeMetadata) SetOwner(x string) {
	md.Owner = x
}

// SetType sets the type of the scope.
func (md *ScopeMetadata) SetType(x int32) {
	md.Type = x
}

// SetFlags sets the flags of the scope.
func (md *ScopeMetadata) SetFlags(x int64) {
	md.Flags = x
}

// SetVersion sets the version of the scope.
func (md *ScopeMetadata) SetVersion(x int32) {
	md.Version = x
}

// SetPrefixStr sets the encoded list of all prefixes.
func (md *ScopeMetadata) SetPrefixStr(x string) {
	md.PrefixStr = x
}

// SetCluster sets the cluster name of the scope.
func (md *ScopeMetadata) SetCluster(x string) {
	md.Cluster = x
}

// SetCreator sets the creator of the scope.
func (md *ScopeMetadata) SetCreator(x string) {
	md.Creator = x
}

// SetCreatedOn sets the create timestamp of the scope.
func (md *ScopeMetadata) SetCreatedOn(x time.Time) {
	md.CreatedOn = x
}

// SetExpiresOn sets the expiration timestamp of the scope.
func (md *ScopeMetadata) SetExpiresOn(x time.Time) {
	md.ExpiresOn = &x
}

// SetExtendCount sets the extend count of the scope.
func (md *ScopeMetadata) SetExtendCount(x int32) {
	md.ExtendCount = x
}

// SetNotifyCount sets the notify count of the scope.
func (md *ScopeMetadata) SetNotifyCount(x int32) {
	md.NotifyCount = x
}

// SetReadMaxRPS sets the max read rate for the scope.
func (md *ScopeMetadata) SetReadMaxRPS(x int32) {
	md.ReadMaxRPS = x
}

// SetWriteMaxRPS sets the max write rate for the scope.
func (md *ScopeMetadata) SetWriteMaxRPS(x int32) {
	md.WriteMaxRPS = x
}
