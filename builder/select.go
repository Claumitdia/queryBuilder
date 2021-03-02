package querybuilder

import (
	"fmt"
	s "strings"
)

type SelectStruct struct {
	selectKeyword     string             // "select"
	columnNames       []ColumnNameStruct //list of columnNames to be appended to select
	FinalSelectPhrase string             // mix of above params
}

//BuildSelect is a function to build the select statement
func (qb *Obj) BuildSelect(columnList []ColumnNameStruct, selectKeyWord string) {
	qb.SQLQuery.SelectPhrase.selectKeyword = selectKeyWord
	qb.SQLQuery.SelectPhrase.columnNames = columnList

	var joinedStr []string
	for _, col := range columnList {
		joinedStr = append(joinedStr, col.FinalColumnNamePhrase)
	}

	finalColumnList := s.Join(joinedStr, ",")

	qb.SQLQuery.SelectPhrase.FinalSelectPhrase = fmt.Sprintf("%s %s ", qb.SQLQuery.SelectPhrase.selectKeyword, finalColumnList)
}
