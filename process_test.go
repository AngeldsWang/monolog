package monolog

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/angeldswang/monolog/consts"
	"github.com/angeldswang/monolog/events"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	client *mongo.Client
	err    error
	ctx    context.Context = context.TODO()
	mono   *Monolog
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestSetup(t *testing.T) {
	client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	assert.Nil(t, err)
	err = client.Database("monolog_snapshot").Collection("test").Drop(ctx)
	assert.Nil(t, err)
}

func TestMonolog(t *testing.T) {
	ces := make([]interface{}, 0)
	t.Run("read fixture data ok", func(t *testing.T) {
		data, err := ioutil.ReadFile("./change_events.json")
		assert.Nil(t, err)
		err = json.Unmarshal(data, &ces)
		assert.Nil(t, err)
	})

	t.Run("monolog process ok", func(t *testing.T) {
		conf := &events.Config{
			DBMap: map[string]string{"monolog": "monolog_snapshot"},
		}
		mono = NewMonolog(client, conf, func(ce events.ChangeEvent) bool {
			return ce.SrcColl == "test_ignore"
		})
		for _, ce := range ces {
			edoc := toD(ce.(map[string]interface{}))
			t.Logf("edoc=%+v\n", edoc)
			edocBytes, err := bson.Marshal(edoc)
			assert.Nil(t, err)
			_, err = mono.Process(ctx, edocBytes)
			assert.Nil(t, err)
		}
	})
}

func toD(mm map[string]interface{}) bson.D {
	d := bson.D{}
	for key, val := range mm {
		if key == consts.FsDocumentKey {
			docKey := val.(map[string]interface{})
			id, ok := docKey["_id"]
			if ok {
				delete(docKey, "_id")
				objectID, _ := primitive.ObjectIDFromHex(id.(string))
				docKey["_id"] = objectID
			}
			d = append(d, bson.E{key, docKey})
		} else if key == consts.FsFullDocument {
			doc := val.(map[string]interface{})
			id, ok := doc["_id"]
			if ok {
				delete(doc, "_id")
				objectID, _ := primitive.ObjectIDFromHex(id.(string))
				doc["_id"] = objectID
			}
			d = append(d, bson.E{key, doc})
		} else {
			d = append(d, bson.E{key, val})
		}
	}

	return d
}
