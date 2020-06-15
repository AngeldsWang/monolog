package operations

import (
	"context"
	"errors"

	"github.com/angeldswang/monolog/events"
	"go.mongodb.org/mongo-driver/mongo"
)

type ReplaceOp struct {
	Client      *mongo.Client
	ChangeEvent *events.ChangeEvent
}

func NewReplaceOp(client *mongo.Client, ce *events.ChangeEvent) *ReplaceOp {
	return &ReplaceOp{
		Client:      client,
		ChangeEvent: ce,
	}
}

func (op *ReplaceOp) Do(ctx context.Context) error {
	if op.ChangeEvent.FullDocument == nil {
		return errors.New("no fulldocument found in change event")
	}

	coll := op.Client.Database(op.ChangeEvent.DstDB).Collection(op.ChangeEvent.DstColl)
	_, err := coll.ReplaceOne(ctx, op.ChangeEvent.DocumentKey, op.ChangeEvent.FullDocument)
	if err != nil {
		return err
	}

	return nil
}
