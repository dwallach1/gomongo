package gomongo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type client struct {
	mclient     *mongo.Client
	mdb         *mongo.Database
	retryPolicy *RetryPolicy
}

func NewClient(mclient *mongo.Client, mdb *mongo.Database, retrypolicy *RetryPolicy) *client {
	return &client{
		mclient,
		mdb,
		retrypolicy,
	}
}

func (c *client) SetRetryPolicy(retryPolicy *RetryPolicy) {
	c.retryPolicy = retryPolicy
}

func (c *client) Find(
	l Logger,
	ctx context.Context,
	collection string,
	filter interface{},
	opts ...*options.FindOptions) (result *mongo.Cursor, err error) {
	result, err = retryable(
		l,
		c.retryPolicy.MaxRetries,
		c.retryPolicy.BackoffStrategy,
		func() (*mongo.Cursor, error) {
			return c.mdb.Collection(collection).Find(
				ctx,
				filter,
				opts...,
			)
		})
	return result, err
}

func (c *client) FindOne(
	l Logger,
	ctx context.Context,
	collection string,
	filter interface{},
	opts ...*options.FindOneOptions) (result *mongo.SingleResult, err error) {
	result, err = retryable(
		l,
		c.retryPolicy.MaxRetries,
		c.retryPolicy.BackoffStrategy,
		func() (*mongo.SingleResult, error) {
			res := c.mdb.Collection(collection).FindOne(
				ctx,
				filter,
				opts...,
			)
			return res, res.Err()
		})
	return result, err
}

func (c *client) InsertOne(
	l Logger,
	ctx context.Context,
	collection string,
	document interface{},
	opts ...*options.InsertOneOptions) (result *mongo.InsertOneResult, err error) {
	result, err = retryable(
		l,
		c.retryPolicy.MaxRetries,
		c.retryPolicy.BackoffStrategy,
		func() (*mongo.InsertOneResult, error) {
			return c.mdb.Collection(collection).InsertOne(
				ctx,
				document,
				opts...,
			)
		})
	return result, err
}

func (c *client) InsertMany(
	l Logger,
	ctx context.Context,
	collection string,
	documents []interface{},
	opts ...*options.InsertManyOptions) (result *mongo.InsertManyResult, err error) {
	result, err = retryable(
		l,
		c.retryPolicy.MaxRetries,
		c.retryPolicy.BackoffStrategy,
		func() (*mongo.InsertManyResult, error) {
			return c.mdb.Collection(collection).InsertMany(
				ctx,
				documents,
				opts...,
			)
		})
	return result, err
}

func (c *client) UpdateOne(
	l Logger,
	ctx context.Context,
	collection string,
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions) (result *mongo.UpdateResult, err error) {
	result, err = retryable(
		l,
		c.retryPolicy.MaxRetries,
		c.retryPolicy.BackoffStrategy,
		func() (*mongo.UpdateResult, error) {
			return c.mdb.Collection(collection).UpdateOne(
				ctx,
				filter,
				update,
				opts...,
			)
		})
	return result, err
}

func (c *client) UpdateMany(
	l Logger,
	ctx context.Context,
	collection string,
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions) (result *mongo.UpdateResult, err error) {
	result, err = retryable(
		l,
		c.retryPolicy.MaxRetries,
		c.retryPolicy.BackoffStrategy,
		func() (*mongo.UpdateResult, error) {
			return c.mdb.Collection(collection).UpdateMany(
				ctx,
				filter,
				update,
				opts...,
			)
		})
	return result, err
}

func (c *client) DeleteOne(
	l Logger,
	ctx context.Context,
	collection string,
	filter interface{},
	opts ...*options.DeleteOptions) (result *mongo.DeleteResult, err error) {
	result, err = retryable(
		l,
		c.retryPolicy.MaxRetries,
		c.retryPolicy.BackoffStrategy,
		func() (*mongo.DeleteResult, error) {
			return c.mdb.Collection(collection).DeleteOne(
				ctx,
				filter,
				opts...,
			)
		})
	return result, err
}

func (c *client) DeleteMany(
	l Logger,
	ctx context.Context,
	collection string,
	filter interface{},
	opts ...*options.DeleteOptions) (result *mongo.DeleteResult, err error) {
	result, err = retryable(
		l,
		c.retryPolicy.MaxRetries,
		c.retryPolicy.BackoffStrategy,
		func() (*mongo.DeleteResult, error) {
			return c.mdb.Collection(collection).DeleteMany(
				ctx,
				filter,
				opts...,
			)
		})
	return result, err
}

func (c *client) Aggregate(
	l Logger,
	ctx context.Context,
	collection string,
	pipeline interface{},
	opts ...*options.AggregateOptions) (result *mongo.Cursor, err error) {
	result, err = retryable(
		l,
		c.retryPolicy.MaxRetries,
		c.retryPolicy.BackoffStrategy,
		func() (*mongo.Cursor, error) {
			return c.mdb.Collection(collection).Aggregate(
				ctx,
				pipeline,
				opts...,
			)
		})
	return result, err
}
