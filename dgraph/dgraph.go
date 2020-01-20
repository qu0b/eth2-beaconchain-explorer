package dgraph

import (
	"context"
	"encoding/json"
	"log"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
)

var client *dgo.Dgraph
var logger = logrus.New().WithField("module", "dgraph")

func Client() *dgo.Dgraph {
	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	dialOpts := append([]grpc.DialOption{},
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	d, err := grpc.Dial("34.76.128.226:9080", dialOpts...)

	if err != nil {
		logger.WithError(err).Panic("Could not Establish a connection to dgraph database")
	}

	logger.Println("connected to dgraph")

	return dgo.NewDgraphClient(
		api.NewDgraphClient(d),
	)
}

func DropData() error {
	err := client.Alter(context.Background(), &api.Operation{DropAll: true})

	if err != nil {
		return err
	} else {
		return nil
	}
}

func SaveNode(node interface{}) {
	ctx := context.Background()

	txn := client.NewTxn()

	mNode, err := json.Marshal(node)
	if err != nil {
		log.Fatal(err)
	}

	_, err = txn.Mutate(ctx, &api.Mutation{SetJson: mNode})
	if err != nil {
		log.Fatal(err)
	}
	err = txn.Commit(ctx)

	if err != nil {
		logger.WithError(err).Error("Could not commit mutation")
	}

	defer txn.Discard(ctx)
}

func Query(q string, object interface{}) {
	ctx := context.Background()
	resp, err := client.NewTxn().Query(ctx, q)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(resp.Json, &object)
	if err != nil {
		log.Fatal(err)
	}
}

// func Setup(c *dgo.Dgraph) {

// 	err := c.Alter(context.Background(), &api.Operation{
// 		Schema: `
// 		name: string @index(term) .
// 		balance: int .
// 		`,
// 	})
// 	if err != nil {
// 		logrus.WithError(err).Error("Could not setup dgraph schema")
// 	}
// }
