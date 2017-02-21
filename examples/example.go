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

package main

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/http"

	"github.com/uber-go/dosa"
	dosaclient "github.com/uber-go/dosa/client"
	connectors "github.com/uber-go/dosa/connectors/yarpc"
	dosarpc "github.com/uber/dosa-idl/.gen/dosa/dosaclient"
)

type Book struct {
	dosa.Entity `dosa:"primaryKey=ISBN"`
	ISBN        string
	Price       float64
}

func main() {
	// entities that will be registered
	entities := []dosa.DomainObject{&Book{}}

	// initialize registrar
	reg, _ := dosa.NewRegistrar("myteam")
	if err := reg.Register(entities...); err != nil {
		// registration will most likely fail as a result of programmer error
		panic("registrar.Register returned an error")
	}

	// setup YARPC connector
	httpTransport := http.NewTransport()
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "skeleton",
		Inbounds: yarpc.Inbounds{
			httpTransport.NewInbound(":8080"),
		},
		Outbounds: yarpc.Outbounds{
			"dosa-gateway": {
				Unary: httpTransport.NewSingleOutbound("http://127.0.0.1:6708"),
			},
		},
	})

	if err := dispatcher.Start(); err != nil {
		panic("dispatcher failed to start")
	}
	defer dispatcher.Stop()

	// All outbound calls require a timeout through yarpc
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn := &connectors.Client{Client: dosarpc.New(dispatcher.ClientConfig("dosa-gateway"))}
	client, _ := dosaclient.NewDefault(reg, conn)
	err := client.Initialize(ctx)
	if err != nil {
		fmt.Printf("err = %n", err.Error())
		return
	}
	writeErr := client.Upsert(ctx, []string{"ISBN", "Price"}, &Book{ISBN: "abc", Price: 23.45})
	if writeErr != nil {
		fmt.Printf("err = %s\n", writeErr)
	}

	newBook := &Book{ISBN: "abc"}
	readErr := client.Read(ctx, []string{"Price"}, newBook)
	if readErr != nil {
		fmt.Printf("err = %s\n", readErr)
	}

	fmt.Printf("Result - ISBN: %s, Price %.2f\n", newBook.ISBN, newBook.Price)
}
