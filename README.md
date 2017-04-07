# DOSA - Declarative Object Storage Abstraction

[![GoDoc][doc-img]][doc]
[![Coverage Status][cov-img]][cov]
[![Build Status][ci-img]][ci]

## Abstract

DOSA open source is not ready for prime time yet. We will announce when it's ready.

[dosa] is a library that provides a distributed
object storage abstraction for applications in golang and
java. It's designed to help with storage discovery and
abstract the underlying database system.

If you'd like to start by writing a small DOSA-enabled
program, check out [the guide][guide/DOSA.md].

## Overview

DOSA is a storage library that supports:

 * methods to store and retrieve go structs
 * struct annotations to describe queries against data
 * tools to create and/or migrate database schemas
 * implementations that serialize requests to remote stateless servers

## Annotations

This project is released under the [MIT License](LICENSE.txt).

[doc-img]: https://godoc.org/github.com/uber/dosa-go?status.svg
[doc]: https://godoc.org/github.com/uber/dosa-go
[ci-img]: https://travis-ci.com/uber-go/dosa.svg?token=zQquuxnrcfs8yizJ2Dcp&branch=master
[ci]: https://travis-ci.com/uber/dosa-go
[cov-img]: https://coveralls.io/repos/uber/dosa-go/badge.svg?branch=master&service=github
[cov]: https://coveralls.io/github/uber/dosa-go?branch=master
