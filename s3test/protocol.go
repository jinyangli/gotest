package s3test

import (
	"fmt"
	"github.com/keybase/go-framed-msgpack-rpc/rpc"
	context "golang.org/x/net/context"
	"log"
)

type GetArg struct {
	Key  string
	Size int
}

type GetRes struct {
	Value []byte
}

type PutArg struct {
	Key   string
	Value []byte
}

type PutRes struct {
	Size int
}

type BlockInterface interface {
	Get(context.Context, GetArg) (GetRes, error)
	Put(context.Context, PutArg) (PutRes, error)
}

func BlockProtocol(i BlockInterface) rpc.Protocol {
	return rpc.Protocol{
		Name: "s3test.1.block",
		Methods: map[string]rpc.ServeHandlerDescription{
			"get": {
				MakeArg: func() interface{} {
					ret := make([]GetArg, 1)
					return &ret
				},
				Handler: func(ctx context.Context, args interface{}) (ret interface{}, err error) {
					typedArgs, ok := args.(*[]GetArg)
					if !ok {
						log.Fatal(fmt.Errorf("not ok\n"))
					}
					ret, err = i.Get(ctx, (*typedArgs)[0])
					return
				},
				MethodType: rpc.MethodCall,
			},
			"put": {
				MakeArg: func() interface{} {
					ret := make([]PutArg, 1)
					return &ret
				},
				Handler: func(ctx context.Context, args interface{}) (ret interface{}, err error) {
					typedArgs, ok := args.(*[]PutArg)
					if !ok {
						log.Fatal(fmt.Errorf("not ok\n"))
					}
					ret, err = i.Put(ctx, (*typedArgs)[0])
					return
				},
				MethodType: rpc.MethodCall,
			},
		},
	}

}

type BlockProtocolClient struct {
	Cli rpc.GenericClient
}

func (c BlockProtocolClient) Get(ctx context.Context, __arg GetArg) (res GetRes, err error) {
	err = c.Cli.Call(ctx, "s3test.1.block.get", []interface{}{__arg}, &res)
	return
}

func (c BlockProtocolClient) Put(ctx context.Context, __arg PutArg) (res PutRes, err error) {
	err = c.Cli.Call(ctx, "s3test.1.block.put", []interface{}{__arg}, &res)
	return
}

/*
type TestLogFactory struct {
}

func (t *TestLogFactory) NewLog() {
	return rpc.SimpleLogOutput
}
*/
