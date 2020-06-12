package events

import (
	"go.mongodb.org/mongo-driver/bson"
)

type ChangeEvent struct {
	DB            string
	Coll          string
	OperationType string
	DocumentKey   bson.D
	UpdatedFields *bson.D
	RemovedFields *bson.A
	FullDocument  *bson.D
}
