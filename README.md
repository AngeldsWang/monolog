# monolog

mongodb change stream replayer

## ChangeEvent
ChangeEvent defined with referring to [change-events/#change-stream-output](https://docs.mongodb.com/manual/reference/change-events/#change-stream-output)
``` go
type ChangeEvent struct {
    SrcDB         string
    SrcColl       string
    DstDB         string
    DstColl       string
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
conf := events.Config{
    DBMap: {"src_db": "dst_db"},
}
client, _ = mongo.Connect(ctx, &conf, options.Client().ApplyURI("mongodb://localhost:27017"))
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
mono := NewMonolog(client, conf, func(ce events.ChangeEvent) bool {
    return ce.SrcColl == "ignore_collection"
})
...
```
