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

package main

import (
	"bytes"
	"io"
	"os"
)

// CaptureFrame is a structure that captures stdout and stderr for inspection by tests
type CaptureFrame struct {
	SaveOut, SaveErr *os.File
	errR, errW       *os.File
	outR, outW       *os.File
	outch, errorch   chan string
	outbuf, errbuf   bytes.Buffer
}

// StartCapture is used to start capturing output
func StartCapture() *CaptureFrame {
	cf := CaptureFrame{}
	cf.SaveErr = os.Stderr
	cf.SaveOut = os.Stdout

	cf.errbuf.Reset()
	cf.errR, cf.errW, _ = os.Pipe()
	os.Stderr = cf.errW
	cf.errorch = make(chan string)
	go func() {
		io.Copy(&cf.errbuf, cf.errR)
		cf.errorch <- cf.errbuf.String()
		close(cf.errorch)

	}()

	cf.outbuf.Reset()
	cf.outR, cf.outW, _ = os.Pipe()
	os.Stdout = cf.outW
	cf.outch = make(chan string)
	go func() {
		io.Copy(&cf.outbuf, cf.outR)
		cf.outch <- cf.outbuf.String()
		close(cf.outch)

	}()

	return &cf
}

// Stop discontinues the capture and returns either the contents of stdout or stderr
func (f *CaptureFrame) stop(which bool) string {
	f.outW.Close()
	f.errW.Close()
	os.Stderr = f.SaveErr
	os.Stdout = f.SaveOut
	stdout := <-f.outch
	stderr := <-f.errorch
	if which == true {
		return stderr
	}
	return stdout
}
