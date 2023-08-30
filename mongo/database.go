package mongo

import (
	"sync"

	"github.com/nextmicro/next-component/mongo/middleware"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Database struct {
	mu sync.Mutex
	ms []middleware.Middleware
	*mongo.Database
}

func (d *Database) Client() *Client {
	d.mu.Lock()
	defer d.mu.Unlock()

	cc := d.Database.Client()
	return &Client{Client: cc, ms: d.ms}
}

// Collection gets a handle for a collection with the given name configured with the given CollectionOptions.
func (d *Database) Collection(name string, opts ...*options.CollectionOptions) *Collection {
	coll := d.Database.Collection(name, opts...)
	return &Collection{Collection: coll, ms: d.ms}
}
