package operations

import (
	"context"
	"errors"

	"github.com/angeldswang/monolog/events"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type UpdateOp struct {
	Client      *mongo.Client
	ChangeEvent *events.ChangeEvent
}

func NewUpdateOp(client *mongo.Client, ce *events.ChangeEvent) *UpdateOp {
	return &UpdateOp{
		Client:      client,
		ChangeEvent: ce,
	}
}

func (op *UpdateOp) Do(ctx context.Context) error {
	if op.ChangeEvent.UpdatedFields == nil && op.ChangeEvent.RemovedFields == nil {
		return errors.New("no update fields found in change event")
	}

	removed := bson.M{}
	for _, f := range *(op.ChangeEvent.RemovedFields) {
		if fs, ok := f.(string); ok {
			removed[fs] = ""
		}
	}
	fullUpdate := bson.D{
		bson.E{"$set", op.ChangeEvent.UpdatedFields},
		bson.E{"$unset", removed},
	}

	coll := op.Client.Database(op.ChangeEvent.DB).Collection(op.ChangeEvent.Coll)
	_, err := coll.UpdateOne(ctx, op.ChangeEvent.DocumentKey, fullUpdate)
	if err != nil {
		return err
	}

	return nil
}
