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
	find       bson.D
	agg        bson.A
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

func (c *QueryCursorBuilder) Find(find bson.D) *QueryCursorBuilder {
	c.find = find
	return c
}

func (c *QueryCursorBuilder) Aggregate(agg bson.A) *QueryCursorBuilder {
	c.agg = agg
	return c
}

func (c *QueryCursorBuilder) BuildFind() (bson.D, *options.FindOptions) {
	n := len(c.sortFields)
	opts := options.Find().SetLimit(c.limit + 1)
	find := c.mergeFind(1)

	switch n {
	case 0:
		return find, opts
	case 1:
		find = append(find, bson.E{c.sortFields[0], bson.D{
			{c.sortNames[0], c.values[0]},
		}})

		return find, opts.SetSort(bson.D{{c.sortFields[0], c.sorts[0]}})
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

		find = append(find, bson.E{"$or", orQuery})
	}

	return find, opts.SetSort(sorts)
}

func (c *QueryCursorBuilder) BuildAggregation() bson.A {
	n := len(c.sortFields)

	switch n {
	case 0:
		return c.createAggregation(nil, nil)
	case 1:
		cursorAgg := bson.D{
			{c.sortFields[0], bson.D{
				{c.sortNames[0], c.values[0]},
			}},
		}

		sorts := bson.D{{c.sortFields[0], c.sorts[0]}}

		return c.createAggregation(cursorAgg, sorts)
	}

	var cursorAgg bson.D
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

		cursorAgg = bson.D{{"$or", orQuery}}

	}

	return c.createAggregation(cursorAgg, sorts)
}

func (c *QueryCursorBuilder) createAggregation(cursorAgg, sort bson.D) bson.A {
	agg := make(bson.A, 0, len(c.agg)+3)
	agg = append(agg, c.agg...)

	if cursorAgg != nil {
		agg = append(agg, bson.D{{"$match", cursorAgg}})
	}

	if sort != nil {
		agg = append(agg, bson.D{{"$sort", sort}})
	}

	if c.limit > 0 {
		agg = append(agg, bson.D{{"$limit", c.limit + 1}})
	}

	return agg
}

func (c *QueryCursorBuilder) mergeFind(n int) bson.D {
	if c.find == nil {
		return make(bson.D, 0, n)
	}

	find := make(bson.D, 0, len(c.find)+n)
	for i := 0; i < len(c.find); i++ {
		find = append(find, c.find[i])
	}

	return find
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
