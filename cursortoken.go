package mongocursor

import (
	"encoding/base64"
)

//go:generate msgp
type CursorFields []interface{}

type SortValueHandler func(index int) []interface{}
type SliceLastItemHandler func(index int)

func CreateToken(currentToken string, limit, length int, sortValueHandler SortValueHandler, sliceLastItemHandler SliceLastItemHandler) (string, string) {
	var firstSortValue []interface{}
	var lastSortValue []interface{}
	slicedLength := length

	if limit != 0 && length > limit {
		slicedLength = length - 1
		sliceLastItemHandler(slicedLength)
	}

	firstSortValue = sortValueHandler(0)
	lastSortValue = sortValueHandler(slicedLength - 1)

	return createNextToken(limit, length, lastSortValue),
		createPrevToken(currentToken, firstSortValue)
}

func createNextToken(limit, length int, sortedValues []interface{}) string {
	if length <= limit || limit == 0 {
		return ""
	}

	cursorFields := make(CursorFields, 1, len(sortedValues)+1)
	cursorFields[0] = 0
	for i := 0; i < len(sortedValues); i++ {
		cursorFields = append(cursorFields, sortedValues[i])
	}

	cfByte, err := cursorFields.MarshalMsg(nil)
	if err != nil {
		panic(err)
	}

	return base64.URLEncoding.EncodeToString(cfByte)
}

func createPrevToken(token string, sortedValues []interface{}) string {
	if token == "" {
		return ""
	}

	cursorFields := make(CursorFields, 1, len(sortedValues)+1)
	cursorFields[0] = 1
	for i := 0; i < len(sortedValues); i++ {
		cursorFields = append(cursorFields, sortedValues[i])
	}

	cfByte, err := cursorFields.MarshalMsg(nil)
	if err != nil {
		return ""
	}

	return base64.URLEncoding.EncodeToString(cfByte)
}

func decodeToken(token string) (CursorFields, error) {
	cfByte, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}

	cf := CursorFields{}
	_, err = cf.UnmarshalMsg(cfByte)

	return cf, err
}
