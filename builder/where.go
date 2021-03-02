package querybuilder

import "fmt"

//WhereStruct will hold the WHERE clause
type WhereStruct struct {
	whereKeyword     string         //where
	WhereOperator    OperatorStruct // can be between , like, equal
	FinalWherePhrase string         //mix of above
}

// BuildWhere is a function to build the where clause
func (qb *Obj) BuildWhere(op *OperatorStruct, whereKeyword string) {
	qb.SQLQuery.WherePhrase.whereKeyword = whereKeyword
	qb.SQLQuery.WherePhrase.WhereOperator = *op
	qb.SQLQuery.WherePhrase.FinalWherePhrase = fmt.Sprintf("%s %s", qb.SQLQuery.WherePhrase.whereKeyword, qb.SQLQuery.WherePhrase.WhereOperator.FinalOperatorPhrase)
}
