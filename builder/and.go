package querybuilder

// AndStruct is a struct to hold the AND clause
// type AndStruct struct {
// 	AndKeyword string //And
// 	// AllowedTypes   []string       // string, int, time etc
// 	AndOperator    OperatorStruct // can be between , like, equal
// 	FinalAndPhrase string         //mix of all above
// 	GroupNum       string
// }

// BuildAnd is a function to build all the and phrase coming in the query  after where and before group by(not in the having)
// func (qb *Obj) BuildAnd() {
// 	for idx := range qb.SQLQuery.AndPhrase {
// 		qb.SQLQuery.AndPhrase[idx].FinalAndPhrase = fmt.Sprintf(" %s %s", qb.SQLQuery.AndPhrase[idx].AndKeyword, qb.SQLQuery.AndPhrase[idx].AndOperator.FinalOperatorPhrase)
// 	}
// }
