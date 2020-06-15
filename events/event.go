package events

import (
	"go.mongodb.org/mongo-driver/bson"
)

type ChangeEvent struct {
	SrcDB         string
	DstDB         string
	SrcColl       string
	DstColl       string
	OperationType string
	DocumentKey   bson.D
	UpdatedFields *bson.D
	RemovedFields *bson.A
	FullDocument  *bson.D
}
