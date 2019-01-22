package mongocursor

import (
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo/options"
)

type QueryCursorBuilder struct {
	sortFields []string
	sorts      []int
	sortNames  []string
	values     []interface{}
	limit      int64
	filter     bson.D
	equalComp  string
}

func NewQueryBuilder(limit int, token string) *QueryCursorBuilder {
	qb := &QueryCursorBuilder{
		sortFields: make([]string, 0, 3),
		sorts:      make([]int, 0, 3),
		sortNames:  make([]string, 0, 3),
		limit:      int64(limit),
	}

	qb.parseToken(token)

	return qb
}

func (c *QueryCursorBuilder) Sort(sortField string, sort int) *QueryCursorBuilder {
	c.sortFields = append(c.sortFields, sortField)
	c.sorts = append(c.sorts, sort)
	if sort == -1 {
		c.sortNames = append(c.sortNames, "$lt")
	} else {
		c.sortNames = append(c.sortNames, "$gt")
	}

	return c
}

func (c *QueryCursorBuilder) Filter(filter bson.D) *QueryCursorBuilder {
	c.filter = filter
	return c
}

func (c *QueryCursorBuilder) BuildFind() (bson.D, *options.FindOptions) {
	n := len(c.sortFields)
	opts := options.Find().SetLimit(c.limit + 1)
	filter := c.mergeFilter(1)

	switch n {
	case 0:
		return filter, opts
	case 1:
		filter = append(filter, bson.E{c.sortFields[0], bson.D{
			{c.sortNames[0], c.values[0]},
		}})

		return filter, opts.SetSort(bson.D{{c.sortFields[0], c.sorts[0]}})
	}

	orQuery := make(bson.A, 0, n)
	sorts := make(bson.D, n)
	for i := 0; i < n; i++ {
		sorts[i] = bson.E{c.sortFields[i], c.sorts[i]}
	}

	if c.values != nil {
		for i := 0; i < n; i++ {
			if i == 0 {
				orQuery = append(orQuery, bson.D{
					{c.sortFields[0], bson.D{
						{c.sortNames[0], c.values[0]},
					}},
				})
			} else {
				orQuery = append(orQuery, bson.D{
					{c.sortFields[0], c.values[0]},
					{c.sortFields[i], bson.D{
						{c.sortNames[i] + c.equalComp, c.values[i]},
					}},
				})
			}
		}

		filter = append(filter, bson.E{"$or", orQuery})
	}

	return filter, opts.SetSort(sorts)
}

func (c *QueryCursorBuilder) BuildAggregation() bson.A {
	n := len(c.sortFields)
	filter := c.mergeFilter(3)

	switch n {
	case 0:
		return c.createAggregation(filter, nil, nil)
	case 1:
		cursorFilter := bson.D{
			{c.sortFields[0], bson.D{
				{c.sortNames[0], c.values[0]},
			}},
		}

		sorts := bson.D{{c.sortFields[0], c.sorts[0]}}

		return c.createAggregation(filter, cursorFilter, sorts)
	}

	var cursorFilter bson.D
	orQuery := make(bson.A, 0, n)
	sorts := make(bson.D, n)
	for i := 0; i < n; i++ {
		sorts[i] = bson.E{c.sortFields[i], c.sorts[i]}
	}

	if c.values != nil {
		for i := 0; i < n; i++ {
			if i == 0 {
				orQuery = append(orQuery, bson.D{
					{c.sortFields[0], bson.D{
						{c.sortNames[0], c.values[0]},
					}},
				})
			} else {
				orQuery = append(orQuery, bson.D{
					{c.sortFields[0], c.values[0]},
					{c.sortFields[i], bson.D{
						{c.sortNames[i] + c.equalComp, c.values[i]},
					}},
				})
			}
		}

		cursorFilter = bson.D{{"$or", orQuery}}

	}

	return c.createAggregation(filter, cursorFilter, sorts)
}

func (c *QueryCursorBuilder) createAggregation(filter, cursorFilter, sort bson.D) bson.A {
	agg := make(bson.A, 0, 4)
	agg = append(agg, filter)

	if cursorFilter != nil {
		agg = append(agg, bson.D{{"$match", cursorFilter}})
	}

	if sort != nil {
		agg = append(agg, bson.D{{"$sort", sort}})
	}

	if c.limit > 0 {
		agg = append(agg, bson.D{{"$limit", c.limit + 1}})
	}

	return agg
}

func (c *QueryCursorBuilder) mergeFilter(n int) bson.D {
	if c.filter == nil {
		return make(bson.D, 0, n)
	}

	filter := make(bson.D, 0, len(c.filter)+n)
	for i := 0; i < len(c.filter); i++ {
		filter = append(filter, c.filter[i])
	}

	return filter
}

func (c *QueryCursorBuilder) parseToken(token string) {
	if token == "" {
		return
	}

	cf, err := decodeToken(token)
	if err != nil {
		panic(err)
	}

	if cf[0].(int64) == 1 {
		c.equalComp = "e"
	}
	c.values = make([]interface{}, 0, len(cf))
	for i := 1; i < len(cf); i++ {
		c.values = append(c.values, cf[i])
	}
}
