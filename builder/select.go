package querybuilder

import (
	"fmt"
	"log"
	s "strings"
)

//SelectStruct is a struct which contains select clause
type SelectStruct struct {
	SelectKeyword     string             // "select"
	ColumnNames       []ColumnNameStruct //list of columnNames to be appended to select
	FinalSelectPhrase string             // mix of above params
}

//BuildSelect is a function to build the select statement
func (qb *Obj) BuildSelect(columnList []ColumnNameStruct) {
	qb.SQLQuery.SelectPhrase.SelectKeyword = qb.SQLLanguageLiterals.SelectKeyword
	qb.SQLQuery.SelectPhrase.ColumnNames = columnList

	log.Println("\nfrom inside sclearelect: ", qb.SQLQuery.SelectPhrase.ColumnNames)
	var joinedStr []string
	for _, col := range columnList {
		joinedStr = append(joinedStr, col.FinalColumnNamePhrase)
	}

	finalColumnList := s.Join(joinedStr, ",")

	qb.SQLQuery.SelectPhrase.FinalSelectPhrase = fmt.Sprintf("%s %s ", qb.SQLQuery.SelectPhrase.SelectKeyword, finalColumnList)
}
