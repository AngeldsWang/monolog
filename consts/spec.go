package consts

// change events field specification
// https://docs.mongodb.com/manual/reference/change-events/
const (
	FsOperationType     = "operationType"
	FsUpdate            = "update"
	FsReplace           = "replace"
	FsNS                = "ns"
	FsTo                = "to"
	FsDB                = "db"
	FsColl              = "coll"
	FsClusterTime       = "clusterTime"
	FsDocumentKey       = "documentKey"
	FsFullDocument      = "fullDocument"
	FsUpdateDescription = "updateDescription"
	FsUpdatedFields     = "updatedFields"
	FsRemovedFields     = "removedFields"
)
