# monolog

mongodb change stream replayer

## ChangeEvent
ChangeEvent defined by [change-events/#change-stream-output](https://docs.mongodb.com/manual/reference/change-events/#change-stream-output)
``` go
type ChangeEvent struct {
	DB            string
	Coll          string
	OperationType string
	DocumentKey   bson.D
	UpdatedFields *bson.D
	RemovedFields *bson.A
	FullDocument  *bson.D
}
```

## Example

- Basic example
``` go
ctx := context.TODO()
client, _ = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
mono := NewMonolog(client)
for _, changeEventBytes := range yourChangeStreamBytes {
    mono.Process(ctx, changeEventBytes)
}
```

- Custom Filter
Filter fuction has been defined as
``` go
type FilterFunc func(ChangeEvent) bool
```

NewMonolog with mounting your custom filters
``` go
mono := NewMonolog(client, func(ce events.ChangeEvent) bool {
    return ce.Coll == "ignore_collection"
})
...
```
