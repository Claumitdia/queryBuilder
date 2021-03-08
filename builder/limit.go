package querybuilder

import "fmt"

// LimitStruct is a struct for limit clause
type LimitStruct struct {
	LimiKeyword      string //limit
	LimitValue       int
	FinalLimitPhrase string //mix of above
}

// BuildLimit is a function to build the limit clause
func (qb *Obj) BuildLimit(limitVal int) {
	qb.SQLQuery.LimitPhrase.LimiKeyword = qb.SQLLanguageLiterals.LimitKeyWord
	qb.SQLQuery.LimitPhrase.LimitValue = limitVal
	qb.SQLQuery.LimitPhrase.FinalLimitPhrase = fmt.Sprintf("%s %d ", qb.SQLQuery.LimitPhrase.LimiKeyword, qb.SQLQuery.LimitPhrase.LimitValue)
}
