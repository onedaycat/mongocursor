package mongocursor

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
)

type data struct {
	ID string `bson:"_id"`
	A  string `bson:"a"`
	B  string `bson:"b"`
}

type result struct {
	Items     []*data
	NextToken string
	PrevToken string
}

func (r *result) String() string {
	str := ""

	for i, p := range r.Items {
		str += fmt.Sprintf("Item%d: id: %s\tname: %s\tn: %s\n", i+1, p.ID, p.A, p.B)
	}

	str += fmt.Sprintf("NextToken:%s\nPrevToken:%s", r.NextToken, r.PrevToken)

	return str
}

var col *mongo.Collection
var isInitData bool

func initClient() *mongo.Collection {
	if col != nil {
		return col
	}

	client, err := mongo.NewClient(os.Getenv("MONGODB_ENDPOINT"))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = cancel
	if err = client.Connect(ctx); err != nil {
		panic(err)
	}

	col = client.Database("mongocursor").Collection("mongocursor")

	return col
}

func initData() {
	if isInitData {
		return
	}

	col.InsertMany(context.Background(), []interface{}{
		data{"5", "a5", "b1"},
		data{"4", "a5", "b0"},
		data{"3", "a5", "b3"},
		data{"2", "a3", "b1"},
		data{"1", "a4", "b2"},
	})
	isInitData = true
}

func getItemsFromFind(limit int, token string) *result {
	find := bson.D{
		{"_id", bson.D{{"$ne", "10"}}},
	}

	query, options := NewBuilder(limit, token).
		Sort("a", -1).
		Sort("_id", 1).
		Find(find).
		BuildFind()
	fmt.Println("Find:", query, "Sort:", options.Sort)

	cursor, err := col.Find(context.Background(), query, options)
	if err != nil {
		panic(err)
	}

	docs := make([]*data, 0, limit+1)
	for cursor.Next(context.Background()) {
		doc := &data{}
		cursor.Decode(doc)
		docs = append(docs, doc)
	}

	nextToken, prevToken := CreateToken(token, limit, len(docs),
		func(index int) []interface{} {
			return []interface{}{docs[index].A, docs[index].ID}
		},
		func(index int) {
			docs = docs[:index]
		},
	)

	return &result{
		Items:     docs,
		NextToken: nextToken,
		PrevToken: prevToken,
	}
}

func TestFind(t *testing.T) {
	initClient()
	initData()

	// testcases := []struct {
	// 	limit int
	// 	token string
	// 	exp   *result
	// }{
	// 	// no token
	// 	{3, "", &result{
	// 		Items: []*data{
	// 			{"3", "a5", "b3"},
	// 			{"4", "a5", "b0"},
	// 		},
	// 		NextToken: "kwCiYTWhNA==",
	// 		PrevToken: "",
	// 	}},
	// 	// next token from [0]
	// 	{3, "kwCiYTWhNA==", &result{
	// 		Items: []*data{
	// 			{"5", "a5", "b1"},
	// 			{"1", "a4", "b2"},
	// 		},
	// 		NextToken: "kwCiYTShMQ==",
	// 		PrevToken: "kwGiYTWhNQ==",
	// 	}},

	// 	// next token from [0]
	// 	{3, "kwGiYTWhNQ==", &result{
	// 		Items: []*data{
	// 			{"3", "a5", "b3"},
	// 			{"4", "a5", "b0"},
	// 		},
	// 		NextToken: "kwCiYTWhNA==",
	// 		PrevToken: "",
	// 	}},
	// }

	// for _, testcase := range testcases {
	// 	// res := getItemsFromFind(testcase.limit, testcase.token)
	// 	// fmt.Println(res)
	// 	// fmt.Println("")
	// 	// require.Equal(t, testcase.exp, res)
	// }

	result1 := getItemsFromFind(3, "")
	fmt.Println(result1)
	fmt.Println("")

	result2 := getItemsFromFind(3, result1.NextToken)
	fmt.Println(result2)
	fmt.Println("")

	result3 := getItemsFromFind(3, result2.PrevToken)
	fmt.Println(result3)
	fmt.Println("")

	result4 := getItemsFromFind(3, result2.NextToken)
	fmt.Println(result4)
	fmt.Println("")
}

func BenchmarkX(b *testing.B) {

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// find := bson.D{
		// 	{"_id", bson.D{{"$ne", "10"}}},
		// }

		NewBuilder(2, "kwCiYTWhNA==").
			Sort("a", -1).
			// Sort("_id", 1).
			// Find(find).
			BuildFind()
	}
}
