package querybuilder

import (
	"fmt"
	"log"
	s "strings"
)

// GroupByStruct struct for group by clause
type GroupByStruct struct {
	GroupByKeyword     string //group by
	GroupByColumns     []ColumnNameStruct
	FinalGroupByPhrase string //mix of above
}

//BuildGroupBy is a function to build the group by  statement
func (qb *Obj) BuildGroupBy(columnList []ColumnNameStruct) {
	qb.SQLQuery.GroupByPhrase.GroupByKeyword = qb.SQLLanguageLiterals.GroupByKeyword
	qb.SQLQuery.GroupByPhrase.GroupByColumns = columnList

	var joinedStr []string
	for _, col := range columnList {
		joinedStr = append(joinedStr, col.FinalColumnNamePhrase)
	}

	finalColumnList := s.Join(joinedStr, ",")

	qb.SQLQuery.GroupByPhrase.FinalGroupByPhrase = fmt.Sprintf(" %s %s ", qb.SQLQuery.GroupByPhrase.GroupByKeyword, finalColumnList)
	log.Println("final group by phrase: ", qb.SQLQuery.GroupByPhrase.FinalGroupByPhrase)
}
