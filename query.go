package mongocursor

import (
	"errors"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo/options"
)

const (
	xlt      = "$lt"
	xgt      = "$gt"
	xor      = "$or"
	xmatch   = "$match"
	xsort    = "$sort"
	xlimit   = "$limit"
	emptyStr = ""
)

type sortField struct {
	name string
	dir  int
	comp string
}

type QueryCursorBuilder struct {
	sortFields  []sortField
	sorts       []int
	sortNames   []string
	values      []interface{}
	limit       int64
	find        bson.D
	agg         bson.A
	token       string
	isPrevToken bool
}

func NewBuilder(limit int, token string) *QueryCursorBuilder {
	c := &QueryCursorBuilder{
		sortFields: make([]sortField, 0, 3),
	}

	if limit < 0 {
		c.limit = 0
	} else {
		c.limit = int64(limit)
	}

	c.parseToken(token)
	c.token = token

	return c
}

func (c *QueryCursorBuilder) Sort(field string, sort int) *QueryCursorBuilder {
	sortField := sortField{
		name: field,
		dir:  sort,
	}

	if c.isPrevToken {
		if sort == -1 {
			sortField.comp = xgt
		} else {
			sortField.comp = xlt
		}
	} else {
		if sort == -1 {
			sortField.comp = xlt
		} else {
			sortField.comp = xgt
		}
	}

	c.sortFields = append(c.sortFields, sortField)

	return c
}

func (c *QueryCursorBuilder) Find(find bson.D) *QueryCursorBuilder {
	c.find = make(bson.D, 0, len(find)+1)

	for i := 0; i < len(find); i++ {
		c.find = append(c.find, find[i])
	}

	return c
}

func (c *QueryCursorBuilder) Aggregate(agg bson.A) *QueryCursorBuilder {
	c.agg = agg

	return c
}

func (c *QueryCursorBuilder) BuildFind() (bson.D, *options.FindOptions) {
	c.validate()
	n := len(c.sortFields)

	var opts *options.FindOptions
	if c.limit != 0 {
		opts = options.Find().SetLimit(c.limit + 1)
	}

	sorts := make(bson.D, n)
	for i := 0; i < n; i++ {
		sorts[i] = bson.E{c.sortFields[i].name, c.sortFields[i].dir}
	}
	opts.SetSort(sorts)

	if c.find == nil {
		c.find = make(bson.D, 0, 1)
	}

	if n == 1 && c.values != nil {
		c.find = append(c.find, c.createSingleQuery())
	} else if c.values != nil {
		c.find = append(c.find, c.createOrQuery(n))
	}

	return c.find, opts
}

func (c *QueryCursorBuilder) BuildAggregate() bson.A {
	c.validate()
	n := len(c.sortFields)

	sorts := make(bson.D, n)
	for i := 0; i < len(c.sortFields); i++ {
		sorts[i] = bson.E{c.sortFields[i].name, c.sortFields[i].dir}
	}

	if n == 1 {
		if c.values == nil {
			return c.createAggregation(nil, sorts)
		}

		return c.createAggregation(bson.D{c.createSingleQuery()}, sorts)
	} else if c.values != nil {
		return c.createAggregation(bson.D{c.createOrQuery(n)}, sorts)
	}

	return c.createAggregation(nil, sorts)
}

func (c *QueryCursorBuilder) validate() {
	if len(c.sortFields) == 0 {
		panic(errors.New("Cursor is required atleast one sort"))
	}

	if c.token != emptyStr {
		if c.values == nil {
			panic(errors.New("No value in token"))
		}

		if len(c.values) < len(c.sortFields) {
			panic(errors.New("Size of value in token must less than number of sort(s)"))
		}
	}
}

func (c *QueryCursorBuilder) createSingleQuery() bson.E {
	return bson.E{c.sortFields[0].name, bson.D{
		{c.sortFields[0].comp, c.values[0]},
	}}
}

func (c *QueryCursorBuilder) createOrQuery(n int) bson.E {
	orQuery := make(bson.A, 0, n)
	for i := 0; i < n; i++ {
		if i == 0 {
			orQuery = append(orQuery, bson.D{
				{c.sortFields[0].name, bson.D{
					{c.sortFields[0].comp, c.values[0]},
				}},
			})
		} else {
			orQuery = append(orQuery, bson.D{
				{c.sortFields[0].name, c.values[0]},
				{c.sortFields[i].name, bson.D{
					{c.sortFields[i].comp, c.values[i]},
				}},
			})
		}
	}

	return bson.E{xor, orQuery}
}

func (c *QueryCursorBuilder) createAggregation(cursorAgg, sort bson.D) bson.A {
	agg := make(bson.A, 0, len(c.agg)+3)
	agg = append(agg, c.agg...)

	if cursorAgg != nil {
		agg = append(agg, bson.D{{xmatch, cursorAgg}})
	}

	if sort != nil {
		agg = append(agg, bson.D{{xsort, sort}})
	}

	if c.limit > 0 {
		agg = append(agg, bson.D{{xlimit, c.limit + 1}})
	}

	return agg
}

func (c *QueryCursorBuilder) parseToken(token string) {
	if token == emptyStr {
		return
	}

	cf := decodeToken(token)
	if cf == nil {
		return
	}

	if cf[0].(int64) == 1 {
		c.isPrevToken = true
	}
	c.values = make([]interface{}, 0, len(cf))
	for i := 1; i < len(cf); i++ {
		c.values = append(c.values, cf[i])
	}
}
