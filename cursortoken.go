package mongocursor

import (
	"encoding/base64"
)

//go:generate msgp
type CursorFields []interface{}

type SortValueHandler func(index int) []interface{}
type SliceLastItemHandler func(index int)

func CreateToken(currentToken string, limit, length int, sortValueHandler SortValueHandler, sliceLastItemHandler SliceLastItemHandler) (string, string, error) {
	var firstSortValue []interface{}
	var lastSortValue []interface{}
	slicedLength := length

	if limit == 0 || length == 0 {
		return emptyStr, emptyStr, nil
	}

	if limit != 0 && length > limit {
		slicedLength = length - 1
		sliceLastItemHandler(slicedLength)
	}

	firstSortValue = sortValueHandler(0)
	lastSortValue = sortValueHandler(slicedLength - 1)

	nextToken, err := createNextToken(limit, length, lastSortValue)
	if err != nil {
		return emptyStr, emptyStr, err
	}

	prevToken := createPrevToken(currentToken, firstSortValue)

	return nextToken, prevToken, nil
}

func CreateNextToken(currentToken string, limit, length int, sortValueHandler SortValueHandler, sliceLastItemHandler SliceLastItemHandler) (string, error) {
	var lastSortValue []interface{}
	slicedLength := length

	if limit == 0 || length == 0 {
		return emptyStr, nil
	}

	if limit != 0 && length > limit {
		slicedLength = length - 1
		sliceLastItemHandler(slicedLength)
	}

	lastSortValue = sortValueHandler(slicedLength - 1)

	return createNextToken(limit, length, lastSortValue)
}

func createNextToken(limit, length int, sortedValues []interface{}) (string, error) {
	if length <= limit || limit == 0 {
		return emptyStr, nil
	}

	cursorFields := make(CursorFields, 1, len(sortedValues)+1)
	cursorFields[0] = 0
	for i := 0; i < len(sortedValues); i++ {
		cursorFields = append(cursorFields, sortedValues[i])
	}

	cfByte, err := cursorFields.MarshalMsg(nil)
	if err != nil {
		return emptyStr, ErrUnableCreateNextToken
	}

	return base64.URLEncoding.EncodeToString(cfByte), nil
}

func createPrevToken(token string, sortedValues []interface{}) string {
	if token == emptyStr {
		return emptyStr
	}

	cursorFields := make(CursorFields, 1, len(sortedValues)+1)
	cursorFields[0] = 1
	for i := 0; i < len(sortedValues); i++ {
		cursorFields = append(cursorFields, sortedValues[i])
	}

	cfByte, err := cursorFields.MarshalMsg(nil)
	if err != nil {
		return emptyStr
	}

	return base64.URLEncoding.EncodeToString(cfByte)
}

func decodeToken(token string) CursorFields {
	cfByte, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return nil
	}

	cf := CursorFields{}
	if _, err = cf.UnmarshalMsg(cfByte); err != nil {
		return nil
	}

	return cf
}
