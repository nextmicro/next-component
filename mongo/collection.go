package mongo

import (
	"context"

	"github.com/nextmicro/next-component/mongo/middleware"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Collection struct {
	*mongo.Collection
	ms []middleware.Middleware
}

func (c *Collection) Middleware(h middleware.Handler) middleware.Handler {
	return middleware.Chain(c.ms...)(h)
}

// Aggregate executes an aggregate command against the collection and returns a cursor over the resulting documents.
//
// The pipeline parameter must be an array of documents, each representing an aggregation stage. The pipeline cannot
// be nil but can be empty. The stage documents must all be non-nil. For a pipeline of bson.D documents, the
// mongo.Pipeline type can be used. See
// https://www.mongodb.com/docs/manual/reference/operator/aggregation-pipeline/#db-collection-aggregate-stages for a list of
// valid stages in aggregations.
//
// The opts parameter can be used to specify options for the operation (see the options.AggregateOptions documentation.)
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/aggregate/.
func (c *Collection) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (res *mongo.Cursor, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.Aggregate(ctx, pipeline, opts...)
	})

	var out any
	out, err = h(ctx, "Aggregate", pipeline)
	if err != nil {
		return
	}

	return out.(*mongo.Cursor), nil
}

// BulkWrite performs a bulk write operation (https://www.mongodb.com/docs/manual/core/bulk-write-operations/).
//
// The models parameter must be a slice of operations to be executed in this bulk write. It cannot be nil or empty.
// All of the models must be non-nil. See the mongo.WriteModel documentation for a list of valid model types and
// examples of how they should be used.
//
// The opts parameter can be used to specify options for the operation (see the options.BulkWriteOptions documentation.)
func (c *Collection) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (res *mongo.BulkWriteResult, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.BulkWrite(ctx, models, opts...)
	})

	var out any
	out, err = h(ctx, "BulkWrite", models)
	if err != nil {
		return
	}

	return out.(*mongo.BulkWriteResult), nil
}

// CountDocuments returns the number of documents in the collection. For a fast count of the documents in the
// collection, see the EstimatedDocumentCount method.
//
// The filter parameter must be a document and can be used to select which documents contribute to the count. It
// cannot be nil. An empty document (e.g. bson.D{}) should be used to count all documents in the collection. This will
// result in a full collection scan.
//
// The opts parameter can be used to specify options for the operation (see the options.CountOptions documentation).
func (c *Collection) CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (res int64, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.CountDocuments(ctx, filter, opts...)
	})

	var out any
	out, err = h(ctx, "CountDocuments", filter)
	if err != nil {
		return
	}

	return out.(int64), nil
}

// DeleteMany executes a delete command to delete documents from the collection.
//
// The filter parameter must be a document containing query operators and can be used to select the documents to
// be deleted. It cannot be nil. An empty document (e.g. bson.D{}) should be used to delete all documents in the
// collection. If the filter does not match any documents, the operation will succeed and a DeleteResult with a
// DeletedCount of 0 will be returned.
//
// The opts parameter can be used to specify options for the operation (see the options.DeleteOptions documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/delete/.
func (c *Collection) DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (res *mongo.DeleteResult, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.DeleteMany(ctx, filter, opts...)
	})

	var out any
	out, err = h(ctx, "DeleteMany", filter)
	if err != nil {
		return
	}

	return out.(*mongo.DeleteResult), nil
}

// DeleteOne executes a delete command to delete at most one document from the collection.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// deleted. It cannot be nil. If the filter does not match any documents, the operation will succeed and a DeleteResult
// with a DeletedCount of 0 will be returned. If the filter matches multiple documents, one will be selected from the
// matched set.
//
// The opts parameter can be used to specify options for the operation (see the options.DeleteOptions documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/delete/.
func (c *Collection) DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (res *mongo.DeleteResult, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.DeleteOne(ctx, filter, opts...)
	})

	var out any
	out, err = h(ctx, "DeleteOne", filter)
	if err != nil {
		return
	}

	return out.(*mongo.DeleteResult), nil
}

// Distinct executes a distinct command to find the unique values for a specified field in the collection.
//
// The fieldName parameter specifies the field name for which distinct values should be returned.
//
// The filter parameter must be a document containing query operators and can be used to select which documents are
// considered. It cannot be nil. An empty document (e.g. bson.D{}) should be used to select all documents.
//
// The opts parameter can be used to specify options for the operation (see the options.DistinctOptions documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/distinct/.
func (c *Collection) Distinct(ctx context.Context, fieldName string, filter interface{}, opts ...*options.DistinctOptions) (res []interface{}, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.Distinct(ctx, fieldName, filter, opts...)
	})

	var out any
	out, err = h(ctx, "Distinct", fieldName, filter)
	if err != nil {
		return
	}

	return out.([]interface{}), nil
}

// Drop drops the collection on the server. This method ignores "namespace not found" errors so it is safe to drop
// a collection that does not exist on the server.
func (c *Collection) Drop(ctx context.Context) (err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return nil, c.Collection.Drop(ctx)
	})

	_, err = h(ctx, "Drop")
	return err
}

// EstimatedDocumentCount executes a count command and returns an estimate of the number of documents in the collection
// using collection metadata.
//
// The opts parameter can be used to specify options for the operation (see the options.EstimatedDocumentCountOptions
// documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/count/.
func (c *Collection) EstimatedDocumentCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (res int64, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.EstimatedDocumentCount(ctx, opts...)
	})

	var out any
	out, err = h(ctx, "EstimatedDocumentCount")
	if err != nil {
		return
	}

	return out.(int64), nil
}

// Find executes a find command and returns a Cursor over the matching documents in the collection.
//
// The filter parameter must be a document containing query operators and can be used to select which documents are
// included in the result. It cannot be nil. An empty document (e.g. bson.D{}) should be used to include all documents.
//
// The opts parameter can be used to specify options for the operation (see the options.FindOptions documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/find/.
func (c *Collection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (res *mongo.Cursor, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.Find(ctx, filter, opts...)
	})

	var out any
	out, err = h(ctx, "Find", filter)
	if err != nil {
		return
	}

	return out.(*mongo.Cursor), nil
}

// FindOne executes a find command and returns a SingleResult for one document in the collection.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// returned. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
// ErrNoDocuments will be returned. If the filter matches multiple documents, one will be selected from the matched set.
//
// The opts parameter can be used to specify options for this operation (see the options.FindOneOptions documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/find/.
func (c *Collection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) (res *mongo.SingleResult) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.FindOne(ctx, filter, opts...), nil
	})

	var out any
	out, _ = h(ctx, "FindOne", filter)
	return out.(*mongo.SingleResult)
}

// FindOneAndDelete executes a findAndModify command to delete at most one document in the collection. and returns the
// document as it appeared before deletion.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// deleted. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
// ErrNoDocuments wil be returned. If the filter matches multiple documents, one will be selected from the matched set.
//
// The opts parameter can be used to specify options for the operation (see the options.FindOneAndDeleteOptions
// documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/findAndModify/.
func (c *Collection) FindOneAndDelete(ctx context.Context, filter interface{}, opts ...*options.FindOneAndDeleteOptions) (res *mongo.SingleResult) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.FindOneAndDelete(ctx, filter, opts...), nil
	})

	var out any
	out, _ = h(ctx, "FindOneAndDelete", filter)
	return out.(*mongo.SingleResult)
}

// FindOneAndReplace executes a findAndModify command to replace at most one document in the collection
// and returns the document as it appeared before replacement.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// replaced. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
// ErrNoDocuments wil be returned. If the filter matches multiple documents, one will be selected from the matched set.
//
// The replacement parameter must be a document that will be used to replace the selected document. It cannot be nil
// and cannot contain any update operators (https://www.mongodb.com/docs/manual/reference/operator/update/).
//
// The opts parameter can be used to specify options for the operation (see the options.FindOneAndReplaceOptions
// documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/findAndModify/.
func (c *Collection) FindOneAndReplace(ctx context.Context, filter, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) (res *mongo.SingleResult) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.FindOneAndReplace(ctx, filter, replacement, opts...), nil
	})

	var out any
	out, _ = h(ctx, "FindOneAndReplace", filter, replacement)
	return out.(*mongo.SingleResult)
}

// FindOneAndUpdate executes a findAndModify command to update at most one document in the collection and returns the
// document as it appeared before updating.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// updated. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
// ErrNoDocuments wil be returned. If the filter matches multiple documents, one will be selected from the matched set.
//
// The update parameter must be a document containing update operators
// (https://www.mongodb.com/docs/manual/reference/operator/update/) and can be used to specify the modifications to be made
// to the selected document. It cannot be nil or empty.
//
// The opts parameter can be used to specify options for the operation (see the options.FindOneAndUpdateOptions
// documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/findAndModify/.
func (c *Collection) FindOneAndUpdate(ctx context.Context, filter, update interface{}, opts ...*options.FindOneAndUpdateOptions) (res *mongo.SingleResult) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.FindOneAndUpdate(ctx, filter, update, opts...), nil
	})

	var out any
	out, _ = h(ctx, "FindOneAndUpdate", filter, update)
	return out.(*mongo.SingleResult)
}

// InsertMany executes an insert command to insert multiple documents into the collection. If write errors occur
// during the operation (e.g. duplicate key error), this method returns a BulkWriteException error.
//
// The documents parameter must be a slice of documents to insert. The slice cannot be nil or empty. The elements must
// all be non-nil. For any document that does not have an _id field when transformed into BSON, one will be added
// automatically to the marshalled document. The original document will not be modified. The _id values for the inserted
// documents can be retrieved from the InsertedIDs field of the returned InsertManyResult.
//
// The opts parameter can be used to specify options for the operation (see the options.InsertManyOptions documentation.)
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/insert/.
func (c *Collection) InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (res *mongo.InsertManyResult, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.InsertMany(ctx, documents, opts...)
	})

	var out any
	out, err = h(ctx, "InsertMany", documents)
	if err != nil {
		return
	}

	return out.(*mongo.InsertManyResult), nil
}

// InsertOne executes an insert command to insert a single document into the collection.
//
// The document parameter must be the document to be inserted. It cannot be nil. If the document does not have an _id
// field when transformed into BSON, one will be added automatically to the marshalled document. The original document
// will not be modified. The _id can be retrieved from the InsertedID field of the returned InsertOneResult.
//
// The opts parameter can be used to specify options for the operation (see the options.InsertOneOptions documentation.)
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/insert/.
func (c *Collection) InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (res *mongo.InsertOneResult, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.InsertOne(ctx, document, opts...)
	})

	var out any
	out, err = h(ctx, "InsertOne", document)
	if err != nil {
		return
	}

	return out.(*mongo.InsertOneResult), nil
}

// ReplaceOne executes an update command to replace at most one document in the collection.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// replaced. It cannot be nil. If the filter does not match any documents, the operation will succeed and an
// UpdateResult with a MatchedCount of 0 will be returned. If the filter matches multiple documents, one will be
// selected from the matched set and MatchedCount will equal 1.
//
// The replacement parameter must be a document that will be used to replace the selected document. It cannot be nil
// and cannot contain any update operators (https://www.mongodb.com/docs/manual/reference/operator/update/).
//
// The opts parameter can be used to specify options for the operation (see the options.ReplaceOptions documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/update/.
func (c *Collection) ReplaceOne(ctx context.Context, filter, replacement interface{}, opts ...*options.ReplaceOptions) (res *mongo.UpdateResult, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.ReplaceOne(ctx, filter, replacement, opts...)
	})

	var out any
	out, err = h(ctx, "ReplaceOne", filter, replacement)
	if err != nil {
		return
	}

	return out.(*mongo.UpdateResult), nil
}

// UpdateMany executes an update command to update documents in the collection.
//
// The filter parameter must be a document containing query operators and can be used to select the documents to be
// updated. It cannot be nil. If the filter does not match any documents, the operation will succeed and an UpdateResult
// with a MatchedCount of 0 will be returned.
//
// The update parameter must be a document containing update operators
// (https://www.mongodb.com/docs/manual/reference/operator/update/) and can be used to specify the modifications to be made
// to the selected documents. It cannot be nil or empty.
//
// The opts parameter can be used to specify options for the operation (see the options.UpdateOptions documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/update/.
func (c *Collection) UpdateMany(ctx context.Context, filter, replacement interface{}, opts ...*options.UpdateOptions) (res *mongo.UpdateResult, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.UpdateMany(ctx, filter, replacement, opts...)
	})

	var out any
	out, err = h(ctx, "UpdateMany", filter, replacement)
	if err != nil {
		return
	}

	return out.(*mongo.UpdateResult), nil
}

// UpdateOne executes an update command to update at most one document in the collection.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// updated. It cannot be nil. If the filter does not match any documents, the operation will succeed and an UpdateResult
// with a MatchedCount of 0 will be returned. If the filter matches multiple documents, one will be selected from the
// matched set and MatchedCount will equal 1.
//
// The update parameter must be a document containing update operators
// (https://www.mongodb.com/docs/manual/reference/operator/update/) and can be used to specify the modifications to be
// made to the selected document. It cannot be nil or empty.
//
// The opts parameter can be used to specify options for the operation (see the options.UpdateOptions documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/update/.
func (c *Collection) UpdateOne(ctx context.Context, filter, replacement interface{}, opts ...*options.UpdateOptions) (res *mongo.UpdateResult, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.UpdateOne(ctx, filter, replacement, opts...)
	})

	var out any
	out, err = h(ctx, "UpdateOne", filter, replacement)
	if err != nil {
		return
	}

	return out.(*mongo.UpdateResult), nil
}

// Watch returns a change stream for all changes on the corresponding collection. See
// https://www.mongodb.com/docs/manual/changeStreams/ for more information about change streams.
//
// The Collection must be configured with read concern majority or no read concern for a change stream to be created
// successfully.
//
// The pipeline parameter must be an array of documents, each representing a pipeline stage. The pipeline cannot be
// nil but can be empty. The stage documents must all be non-nil. See https://www.mongodb.com/docs/manual/changeStreams/ for
// a list of pipeline stages that can be used with change streams. For a pipeline of bson.D documents, the
// mongo.Pipeline{} type can be used.
//
// The opts parameter can be used to specify options for change stream creation (see the options.ChangeStreamOptions
// documentation).
func (c *Collection) Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (res *mongo.ChangeStream, err error) {
	h := c.Middleware(func(ctx context.Context, cmdName string, req ...interface{}) (interface{}, error) {
		return c.Collection.Watch(ctx, pipeline, opts...)
	})

	var out any
	out, err = h(ctx, "Watch", pipeline)
	if err != nil {
		return
	}

	return out.(*mongo.ChangeStream), nil
}
