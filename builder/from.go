package querybuilder

import "fmt"

//FromStruct will hold the from clause
type FromStruct struct {
	FromKeyword     string //"from"
	FromSource      string //tableName
	FinalFromPhrase string //mix of above
}

// BuildFrom is a function to build the from clause
func (qb *Obj) BuildFrom(fromSource string, fromKeyword string) {
	qb.SQLQuery.FromPhrase.FromKeyword = fromKeyword
	qb.SQLQuery.FromPhrase.FromSource = fromSource
	qb.SQLQuery.FromPhrase.FinalFromPhrase = fmt.Sprintf("%s %s ", qb.SQLQuery.FromPhrase.FromKeyword, qb.SQLQuery.FromPhrase.FromSource)
}
