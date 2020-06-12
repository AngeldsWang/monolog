package monolog

import (
	"context"
	"errors"

	"github.com/angeldswang/monolog/consts"
	"github.com/angeldswang/monolog/events"
	"github.com/angeldswang/monolog/operations"
	"github.com/golang/gddo/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Monolog struct {
	Client  *mongo.Client
	Filters []events.FilterFunc
}

func NewMonolog(client *mongo.Client, filters ...events.FilterFunc) *Monolog {
	return &Monolog{
		Client:  client,
		Filters: filters,
	}
}

func (mono *Monolog) Process(ctx context.Context, data []byte) error {
	entry := &bson.D{}
	if err := bson.Unmarshal(data, entry); err != nil {
		log.Error(ctx, "bson Unmarshal failed", "err", err)
		return err
	}

	changeEvent, err := parseEntry(ctx, entry)
	if err != nil {
		log.Error(ctx, "parse entry failed", "err", err)
		return err
	}

	for _, filter := range mono.Filters {
		if filter(*changeEvent) {
			return nil
		}
	}

	return mono.process(ctx, changeEvent)
}

func (mono *Monolog) process(ctx context.Context, changeEvent *events.ChangeEvent) error {
	return operations.NewOperator(mono.Client, changeEvent).Do(ctx)
}

func parseEntry(ctx context.Context, entry *bson.D) (*events.ChangeEvent, error) {
	ns, ok := entry.Map()[consts.FsNS].(bson.D)
	if !ok {
		return nil, errors.New("invalid ns fields")
	}
	db, ok := ns.Map()[consts.FsDB].(string)
	if !ok {
		return nil, errors.New("invalid database name")
	}
	collection, ok := ns.Map()[consts.FsColl].(string)
	if !ok {
		return nil, errors.New("invalid collection name")
	}

	operationType, ok := entry.Map()[consts.FsOperationType].(string)
	if !ok {
		return nil, errors.New("invalid operationType")
	}

	documentKey, ok := entry.Map()[consts.FsDocumentKey].(bson.D)
	if !ok {
		return nil, errors.New("invalid document key")
	}

	changeEvent := &events.ChangeEvent{
		DB:            db,
		Coll:          collection,
		OperationType: operationType,
		DocumentKey:   documentKey,
	}

	if entry.Map()[consts.FsFullDocument] != nil {
		fullDocument := entry.Map()[consts.FsFullDocument].(bson.D)
		changeEvent.FullDocument = &fullDocument
	}

	if operationType == consts.FsUpdate {
		updates, ok := entry.Map()[consts.FsUpdateDescription].(bson.D)
		if !ok {
			return nil, errors.New("invalid updateDescription")
		}
		uf := updates.Map()[consts.FsUpdatedFields]
		if uf != nil {
			updatedFields := uf.(bson.D)
			changeEvent.UpdatedFields = &updatedFields
		}
		rf := updates.Map()[consts.FsRemovedFields]
		if rf != nil {
			removedFields := rf.(bson.A)
			changeEvent.RemovedFields = &removedFields
		}
	}

	return changeEvent, nil
}
