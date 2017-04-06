package mongoByte

import (
	"time"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"context"
	"github.com/SnaphyLabs/SnaphyByte/resource"
	"github.com/SnaphyLabs/SnaphyByte/models"
	"github.com/SnaphyLabs/SnaphyByte/collections"
)




type mongoItem struct {
	ID      interface{}            `bson:"_id"`
	ETag    string                 `bson:"_etag"`
	Updated time.Time              `bson:"_updated"`
	Created time.Time              `bson:"_created"`
	Type    string                 `bson:"_collection_type_byte"`
	Payload map[string]interface{} `bson:",inline"`
}




// newMongoItem converts a resource.Item into a mongoItem
func newMongoItem(i *models.BaseModel) *mongoItem {
	// Filter out id from the payload so we don't store it twice
	p := map[string]interface{}{}
	for k, v := range i.Payload {
		if k != "id" {
			p[k] = v
		}
	}
	return &mongoItem{
		ID:      i.ID,
		ETag:    i.ETag,
		Updated: i.Updated,
		Created: i.Created,
		Type:    i.Type,
		Payload: p,
	}
}




// newItem converts a back mongoItem into a *models.BaseModel
func newItem(i *mongoItem) *models.BaseModel {
	// Add the id back (we use the same map hoping the mongoItem won't be stored back)
	i.Payload["id"] = i.ID
	return &models.BaseModel{
		ID:      i.ID,
		ETag:    i.ETag,
		Updated: i.Updated,
		Created: i.Created,
		Payload: i.Payload,
		Type: 	 i.Type,
	}
}




// Handler handles resource storage in a MongoDB collection.
type Handler func(ctx context.Context) (*mgo.Collection, error)



// NewHandler creates an new mongo handler
func NewHandler(s *mgo.Session, db, collection string) Handler {
	c := func() *mgo.Collection {
		return s.DB(db).C(collection)
	}
	return func(ctx context.Context) (*mgo.Collection, error) {
		return c(), nil
	}
}



// C returns the mongo collection managed by this storage handler
// from a Copy() of the mgo session.
func (m Handler) c(ctx context.Context) (*mgo.Collection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, err := m(ctx)
	if err != nil {
		return nil, err
	}
	// With mgo, session.Copy() pulls a connection from the connection pool
	s := c.Database.Session.Copy()
	// Ensure safe mode is enabled in order to get errors
	s.EnsureSafe(&mgo.Safe{})
	// Set a timeout to match the context deadline if any
	if deadline, ok := ctx.Deadline(); ok {
		timeout := deadline.Sub(time.Now())
		if timeout <= 0 {
			timeout = 0
		}
		s.SetSocketTimeout(timeout)
		s.SetSyncTimeout(timeout)
	}
	c.Database.Session = s
	return c, nil
}



// close returns a mgo.Collection's session to the connection pool.
func (m Handler) close(c *mgo.Collection) {
	c.Database.Session.Close()
}



// Insert inserts new items in the mongo collection
func (m Handler) Insert(ctx context.Context, items []*models.BaseModel) error {
	mItems := make([]interface{}, len(items))
	for i, item := range items {
		mItems[i] = newMongoItem(item)
	}
	c, err := m.c(ctx)
	if err != nil {
		return err
	}
	defer m.close(c)
	err = c.Insert(mItems...)
	if mgo.IsDup(err) {
		// Duplicate ID key
		err = resource.ErrConflict
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}




// Update replace an item by a new one in the mongo collection
func (m Handler) Update(ctx context.Context, item *models.BaseModel, original *models.BaseModel) error {
	mItem := newMongoItem(item)
	c, err := m.c(ctx)
	if err != nil {
		return err
	}
	defer m.close(c)
	//Update where
	err = c.Update(bson.M{"_id": original.ID, "_etag": original.ETag, "_type": original.Type}, mItem)
	if err == mgo.ErrNotFound {
		// Determine if the item is not found or if the item is found but etag missmatch
		var count int
		count, err = c.FindId(original.ID).Count()
		if err != nil {
			// The find returned an unexpected err, just forward it with no mapping
		} else if count == 0 {
			err = resource.ErrNotFound
		} else if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			// If the item were found, it means that its etag didn't match
			err = resource.ErrConflict
		}
	}
	return err
}



// Update replace an item by a new one in the mongo collection
func (m Handler) Set(ctx context.Context, item *models.BaseModel, original *models.BaseModel) error {
	//Calculate new ETAG..
	mItem := newMongoItem(item)
	c, err := m.c(ctx)
	if err != nil {
		return err
	}
	defer m.close(c)
	/*
	db.products.update(
	   { _id: 100 },
	   { $set:
	      {
		"tags.1": "rain gear",
		"ratings.0.rating": 2
	      }
	   }
	)
	*/
	//Update where
	err = c.Update(bson.M{"_id": original.ID, "_etag": original.ETag, "_type": original.Type}, bson.M{ "$set": mItem} )
	if err == mgo.ErrNotFound {
		// Determine if the item is not found or if the item is found but etag missmatch
		var count int
		count, err = c.FindId(original.ID).Count()
		if err != nil {
			// The find returned an unexpected err, just forward it with no mapping
		} else if count == 0 {
			err = resource.ErrNotFound
		} else if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			// If the item were found, it means that its etag didn't match
			err = resource.ErrConflict
		}
	}
	return err
}



// Delete deletes an item from the mongo collection
func (m Handler) Delete(ctx context.Context, item *models.BaseModel) error {
	c, err := m.c(ctx)
	if err != nil {
		return err
	}
	defer m.close(c)
	err = c.Remove(bson.M{"_id": item.ID, "_etag": item.ETag,  "_type": item.Type})
	if err == mgo.ErrNotFound {
		// Determine if the item is not found or if the item is found but etag missmatch
		var count int
		count, err = c.FindId(item.ID).Count()
		if err != nil {
			// The find returned an unexpected err, just forward it with no mapping
		} else if count == 0 {
			err = resource.ErrNotFound
		} else if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			// If the item were found, it means that its etag didn't match
			err = resource.ErrConflict
		}
	}
	return err
}



// Clear clears all items from the mongo collection matching the lookup
func (m Handler) Clear(ctx context.Context, lookup *resource.Lookup) (int, error) {
	q, err := getQuery(lookup)
	if err != nil {
		return 0, err
	}
	c, err := m.c(ctx)
	if err != nil {
		return 0, err
	}
	defer m.close(c)
	info, err := c.RemoveAll(q)
	if err != nil {
		return 0, err
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return info.Removed, nil
}



// Find items from the mongo collection matching the provided lookup
func (m Handler) Find(ctx context.Context, lookup *resource.Lookup, offset, limit int) (*collections.BaseModelList, error) {
	q, err := getQuery(lookup)
	if err != nil {
		return nil, err
	}
	s := getSort(lookup)
	c, err := m.c(ctx)
	if err != nil {
		return nil, err
	}
	defer m.close(c)
	var mItem mongoItem
	query := c.Find(q).Sort(s...)

	//Add specific fields only..
	if lookup.Field() != nil{
		//Select fields only to display..
		fields, err := translateField(lookup.Field())
		if err != nil{
			return nil, err
		}
		query.Select(fields)
	}

	if offset > 0 {
		query.Skip(offset)
	}
	if limit >= 0 {
		query.Limit(limit)
	}
	// Apply context deadline if any
	if dl, ok := ctx.Deadline(); ok {
		dur := dl.Sub(time.Now())
		if dur < 0 {
			dur = 0
		}
		//Set expiry time for query execution..
		query.SetMaxTime(dur)
	}
	// Perform request
	iter := query.Iter()
	// Total is set to -1 because we have no easy way with Mongodb to to compute this value
	// without performing two requests.
	list := &collections.BaseModelList{Total: -1, Models: []*models.BaseModel{}}
	for iter.Next(&mItem) {
		// Check if context is still ok before to continue
		if err = ctx.Err(); err != nil {
			// TODO bench this as net/context is using mutex under the hood
			iter.Close()
			return nil, err
		}
		list.Models = append(list.Models, newItem(&mItem))
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	// If the number of returned elements is lower than requested limit, or not
	// limit is requested, we can deduce the total number of element for free.
	if limit == -1 || len(list.Models) <= limit {
		list.Total = offset + len(list.Models)
	}
	return list, err
}



// Count counts the number items matching the lookup filter
func (m Handler) Count(ctx context.Context, lookup *resource.Lookup) (int, error) {
	q, err := getQuery(lookup)
	if err != nil {
		return -1, err
	}
	c, err := m.c(ctx)
	if err != nil {
		return -1, err
	}
	defer m.close(c)
	query := c.Find(q)
	// Apply context deadline if any
	if dl, ok := ctx.Deadline(); ok {
		dur := dl.Sub(time.Now())
		if dur < 0 {
			dur = 0
		}
		query.SetMaxTime(dur)
	}
	return query.Count()
}








