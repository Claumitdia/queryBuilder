package querybuilder

import "fmt"

// OrStruct is a struct to hold the OR clause
type OrStruct struct {
	OrKeyword     string         //or
	AllowedTypes  []string       // string, int, time etc
	OrOperator    OperatorStruct // can be between , like, equal
	FinalOrPhrase string
	GroupNum      string
}

// BuildOr is a function to build all the and phrase coming in the query  after where and before group by(not in the having)
func (qb *Obj) BuildOr() {
	for idx := range qb.SQLQuery.OrPhrase {
		qb.SQLQuery.OrPhrase[idx].FinalOrPhrase = fmt.Sprintf(" %s %s", qb.SQLQuery.OrPhrase[idx].OrKeyword, qb.SQLQuery.OrPhrase[idx].OrOperator.FinalOperatorPhrase)
	}
}
