package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/onedaycat/mongocursor"
)

type Pag struct {
	ID   string `bson:"_id"`
	Name string `bson:"name"`
	N    int    `bson:"n"`
}

type Result struct {
	Items     []*Pag
	NextToken string
	PrevToken string
}

func (r *Result) String() string {
	str := ""

	for i, p := range r.Items {
		str += fmt.Sprintf("Item%d: id: %s\tname: %s\tn: %d\n", i+1, p.ID, p.Name, p.N)
	}

	str += fmt.Sprintf("NextToken:%s\nPrevToken:%s", r.NextToken, r.PrevToken)

	return str
}

func getItemsFromAggreation(ctx context.Context, client *mongo.Client, limit int, token string) *Result {
	filter := bson.D{
		{"$match", bson.D{
			{"_id", bson.D{{"$ne", "10"}}}},
		},
	}

	query := mongocursor.NewQueryBuilder(limit, token).
		Sort("name", -1).
		Sort("_id", 1).
		Filter(filter).
		BuildAggregation()
	fmt.Println("Filter:", query)

	cursor, err := client.Database("testpage").Collection("pag").Aggregate(ctx, query)
	if err != nil {
		panic(err)
	}

	pags := make([]*Pag, 0, limit+1)
	for cursor.Next(context.Background()) {
		doc := &Pag{}
		cursor.Decode(doc)
		pags = append(pags, doc)
	}

	nextToken, prevToken := mongocursor.CreateToken(token, limit, len(pags),
		func(index int) []interface{} {
			return []interface{}{pags[index].Name, pags[index].ID}
		},
		func(index int) {
			pags = pags[:index]
		},
	)

	return &Result{
		Items:     pags,
		NextToken: nextToken,
		PrevToken: prevToken,
	}
}

func getItemsFromFind(ctx context.Context, client *mongo.Client, limit int, token string) *Result {
	filter := bson.D{
		{"_id", bson.D{{"$ne", "10"}}},
	}

	query, options := mongocursor.NewQueryBuilder(limit, token).
		Sort("name", -1).
		Sort("_id", 1).
		Filter(filter).
		BuildFind()
	fmt.Println("Filter:", query)

	cursor, err := client.Database("testpage").Collection("pag").Find(ctx, query, options)
	if err != nil {
		panic(err)
	}

	pags := make([]*Pag, 0, limit+1)
	for cursor.Next(context.Background()) {
		doc := &Pag{}
		cursor.Decode(doc)
		pags = append(pags, doc)
	}

	nextToken, prevToken := mongocursor.CreateToken(token, limit, len(pags),
		func(index int) []interface{} {
			return []interface{}{pags[index].Name, pags[index].ID}
		},
		func(index int) {
			pags = pags[:index]
		},
	)

	return &Result{
		Items:     pags,
		NextToken: nextToken,
		PrevToken: prevToken,
	}
}

func main() {
	client, err := mongo.NewClient(os.Getenv("MONGODB_ENDPOINT"))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = cancel
	if err = client.Connect(ctx); err != nil {
		panic(err)
	}

	result1 := getItemsFromFind(ctx, client, 2, "")
	fmt.Println(result1)
	fmt.Println("")

	result2 := getItemsFromFind(ctx, client, 2, result1.NextToken)
	fmt.Println(result2)
	fmt.Println("")

	result3 := getItemsFromFind(ctx, client, 2, result2.PrevToken)
	fmt.Println(result3)
	fmt.Println("")

	result4 := getItemsFromFind(ctx, client, 2, result2.NextToken)
	fmt.Println(result4)
	fmt.Println("")

	result5 := getItemsFromAggreation(ctx, client, 2, "")
	fmt.Println(result5)
	fmt.Println("")

	result6 := getItemsFromAggreation(ctx, client, 2, result5.NextToken)
	fmt.Println(result6)
	fmt.Println("")

	result7 := getItemsFromAggreation(ctx, client, 2, result6.PrevToken)
	fmt.Println(result7)
	fmt.Println("")

	result8 := getItemsFromAggreation(ctx, client, 2, result6.NextToken)
	fmt.Println(result8)
	fmt.Println("")
}
