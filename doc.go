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

// Package dosa is the DOSA - Declarative Object Storage Abstraction.
//
// Abstract
//
// [dosa][dosa-spec] is a library that provides a distributed
// object storage abstraction for applications in golang and
// java. It's designed to help with storage discovery and
// abstract the underlying database system.
//
//
// If you'd like to start by writing a small DOSA-enabled
// program, check out [the guide][guide/DOSA.md].
//
//
// Overview
//
// DOSA is a storage library that supports:
//
// • methods to store and retrieve go structs
//
// • struct annotations to describe queries against data
//
// • tools to create and/or migrate database schemas
//
// • implementations that serialize requests to remote stateless servers
//
// Annotations
//
// This project is released under the MIT License (LICENSE.md).
//
//
package dosa
