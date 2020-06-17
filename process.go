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
	Config  *events.Config
	Filters []events.FilterFunc
}

func NewMonolog(client *mongo.Client, conf *events.Config, filters ...events.FilterFunc) *Monolog {
	return &Monolog{
		Client:  client,
		Config:  conf,
		Filters: filters,
	}
}

func (mono *Monolog) Process(ctx context.Context, data []byte) (*events.ChangeEvent, error) {
	entry := &bson.D{}
	if err := bson.Unmarshal(data, entry); err != nil {
		log.Error(ctx, "bson Unmarshal failed", "err", err)
		return nil, err
	}

	changeEvent, err := mono.parseEntry(ctx, entry)
	if err != nil {
		log.Error(ctx, "parse entry failed", "err", err)
		return nil, err
	}

	for _, filter := range mono.Filters {
		if filter(*changeEvent) {
			return changeEvent, nil
		}
	}

	return changeEvent, mono.process(ctx, changeEvent)
}

func (mono *Monolog) process(ctx context.Context, changeEvent *events.ChangeEvent) error {
	return operations.NewOperator(mono.Client, changeEvent).Do(ctx)
}

func (mono *Monolog) parseEntry(ctx context.Context, entry *bson.D) (*events.ChangeEvent, error) {
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

	dstDB, ok := mono.Config.DBMap[db]
	if !ok {
		return nil, errors.New("invalid db mapping config")
	}

	var dstColl = collection
	if len(mono.Config.CollMap) > 0 {
		dstColl, _ = mono.Config.CollMap[collection]
	}

	changeEvent := &events.ChangeEvent{
		SrcDB:         db,
		SrcColl:       collection,
		DstDB:         dstDB,
		DstColl:       dstColl,
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
