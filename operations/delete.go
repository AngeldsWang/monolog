package operations

import (
	"context"

	"github.com/angeldswang/monolog/events"
	"go.mongodb.org/mongo-driver/mongo"
)

type DeleteOp struct {
	Client      *mongo.Client
	ChangeEvent *events.ChangeEvent
}

func NewDeleteOp(client *mongo.Client, ce *events.ChangeEvent) *DeleteOp {
	return &DeleteOp{
		Client:      client,
		ChangeEvent: ce,
	}
}

func (op *DeleteOp) Do(ctx context.Context) error {
	coll := op.Client.Database(op.ChangeEvent.DstDB).Collection(op.ChangeEvent.DstColl)
	_, err := coll.DeleteOne(ctx, op.ChangeEvent.DocumentKey)
	if err != nil {
		return err
	}

	return nil
}
