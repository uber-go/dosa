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
	reg, _ := dosa.NewRegistrar("myteam.service")
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
	client.Initialize(ctx)
	writeErr := client.Upsert(ctx, nil, &Book{ISBN: "abc", Price: 23.45})
	if writeErr != nil {
		fmt.Printf("err = %s\n", writeErr)
	}

	newBook := &Book{ISBN: "abc"}
	readErr := client.Read(ctx, nil, newBook)
	if readErr != nil {
		fmt.Printf("err = %s\n", readErr)
	}

	fmt.Printf("Result - ISBN: %s, Price %.2f\n", newBook.ISBN, newBook.Price)
}
