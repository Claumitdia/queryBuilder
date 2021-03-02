package querybuilder

import (
	"fmt"
)

//OperatorStruct is a struct for any operator defined in literal , anything in the form : <colName> <operator> <colVal>
type OperatorStruct struct {
	OperatorKeyword     string // =, between , like, > ,>=, <, <=, <>, in
	LeftColumnName      ColumnNameStruct
	RightColumnValue    string //converted to wahtever later
	FinalOperatorPhrase string
}

//BuildOperator will build the operator clause
func (op *OperatorStruct) BuildOperator(leftColName ColumnNameStruct, rightVal string, opKeyWord string, languageKeyword string) {
	op.LeftColumnName = leftColName
	op.RightColumnValue = rightVal
	op.OperatorKeyword = opKeyWord
	if op.LeftColumnName.columnType == "timestamp" {
		if languageKeyword == DruidSQLLanguageLiterals.Language {
			op.FinalOperatorPhrase = fmt.Sprintf(op.OperatorKeyword, op.LeftColumnName.FinalColumnNamePhrase, fmt.Sprintf(DruidSQLLanguageLiterals.TimestampLiteral, op.RightColumnValue))
		} else if languageKeyword == PGSQLLanguageLiterals.Language {
			op.FinalOperatorPhrase = fmt.Sprintf(op.OperatorKeyword, op.LeftColumnName.FinalColumnNamePhrase, fmt.Sprintf(PGSQLLanguageLiterals.TimestampLiteral, op.RightColumnValue))
		}
	} else {
		op.FinalOperatorPhrase = fmt.Sprintf(op.OperatorKeyword, op.LeftColumnName.FinalColumnNamePhrase, op.RightColumnValue)
	}

}
