package operations

import (
	"context"

	"github.com/angeldswang/monolog/consts"
	"github.com/angeldswang/monolog/events"
	"go.mongodb.org/mongo-driver/mongo"
)

type Operation interface {
	Do(context.Context) error
}

type Operator struct {
	Client      *mongo.Client
	ChangeEvent *events.ChangeEvent
}

func NewOperator(client *mongo.Client, ce *events.ChangeEvent) *Operator {
	return &Operator{
		Client:      client,
		ChangeEvent: ce,
	}
}

func (op *Operator) Do(ctx context.Context) error {
	switch op.ChangeEvent.OperationType {
	case consts.OpInsert:
		return NewInsertOp(op.Client, op.ChangeEvent).Do(ctx)
	case consts.OpDelete:
		return NewDeleteOp(op.Client, op.ChangeEvent).Do(ctx)
	case consts.OpUpdate:
		return NewUpdateOp(op.Client, op.ChangeEvent).Do(ctx)
	case consts.OpReplace:
		return NewReplaceOp(op.Client, op.ChangeEvent).Do(ctx)
	}

	return nil
}
