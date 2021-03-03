package querybuilder

import (
	"fmt"
	"log"
)

//OperatorStruct is a struct for any operator defined in literal , anything in the form : <colName> <operator> <colVal>
type OperatorStruct struct {
	OperatorKeyword        string // =, between , like, > ,>=, <, <=, <>, in
	LeftColumnName         ColumnNameStruct
	RightColumnValueString string //converted to wahtever later
	RightColumnValueInt    float64
	FinalOperatorPhrase    string
}

//BuildOperatorString will build the operator clause
func (op *OperatorStruct) BuildOperatorString(leftColName ColumnNameStruct, rightVal string, opKeyWord string, languageKeyword string) {
	op.LeftColumnName = leftColName
	op.RightColumnValueString = rightVal
	op.OperatorKeyword = opKeyWord
	if op.LeftColumnName.columnType == "timestamp" {
		if languageKeyword == DruidSQLLanguageLiterals.Language {
			op.FinalOperatorPhrase = fmt.Sprintf(op.OperatorKeyword, op.LeftColumnName.FinalColumnNamePhrase, fmt.Sprintf(DruidSQLLanguageLiterals.TimestampLiteral, op.RightColumnValueString))
		} else if languageKeyword == PGSQLLanguageLiterals.Language {
			op.FinalOperatorPhrase = fmt.Sprintf(op.OperatorKeyword, op.LeftColumnName.FinalColumnNamePhrase, fmt.Sprintf(PGSQLLanguageLiterals.TimestampLiteral, op.RightColumnValueString))
		}
	} else {
		op.FinalOperatorPhrase = fmt.Sprintf(op.OperatorKeyword, op.LeftColumnName.FinalColumnNamePhrase, op.RightColumnValueString)
	}

}

// BuildOperatorInt will build int column operator
func (op *OperatorStruct) BuildOperatorInt(leftColName ColumnNameStruct, rightVal float64, opKeyWord string, languageKeyword string) {
	op.LeftColumnName = leftColName
	op.RightColumnValueInt = rightVal
	op.OperatorKeyword = opKeyWord
	log.Println("operator literal string :", op.OperatorKeyword)
	op.FinalOperatorPhrase = fmt.Sprintf(op.OperatorKeyword, op.LeftColumnName.FinalColumnNamePhrase, op.RightColumnValueInt)
}
