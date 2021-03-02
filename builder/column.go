package querybuilder

import (
	"fmt"
)

// ColumnNameStruct is a struct to get function name around columns name
type ColumnNameStruct struct {
	columnName            string //actual columnname
	columnType            string
	columnAlias           string             //AS "something"
	columnFunction        ColumnFunctionType //function tobe applied on column, can beleft blank
	FinalColumnNamePhrase string             //mix of above
}

// BuildColumnNameStructObj is a function to build the function object final phrase
func (cns *ColumnNameStruct) BuildColumnNameStructObj(columnName string, columnType string, columnAlias string, columnFunction ColumnFunctionType) {
	cns.columnName = columnName
	cns.columnFunction = columnFunction
	cns.columnType = columnType
	cns.columnAlias = columnAlias
	if cns.columnAlias == "" {
		cns.FinalColumnNamePhrase = fmt.Sprintf(cns.columnFunction.FinalColumnFunctionPhrase, cns.columnName)
	} else {
		cns.FinalColumnNamePhrase = fmt.Sprintf(cns.columnFunction.FinalColumnFunctionPhrase+" AS %s", cns.columnName, cns.columnAlias)
	}
}
