package operations

import (
	"context"
	"errors"

	"github.com/angeldswang/monolog/events"
	"go.mongodb.org/mongo-driver/mongo"
)

type InsertOp struct {
	Client      *mongo.Client
	ChangeEvent *events.ChangeEvent
}

func NewInsertOp(client *mongo.Client, ce *events.ChangeEvent) *InsertOp {
	return &InsertOp{
		Client:      client,
		ChangeEvent: ce,
	}
}

func (op *InsertOp) Do(ctx context.Context) error {
	if op.ChangeEvent.FullDocument == nil {
		return errors.New("no fulldocument found in change event")
	}

	coll := op.Client.Database(op.ChangeEvent.DstDB).Collection(op.ChangeEvent.DstColl)
	_, err := coll.InsertOne(ctx, op.ChangeEvent.FullDocument)
	if err != nil {
		return err
	}

	return nil
}
