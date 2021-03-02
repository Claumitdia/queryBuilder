package querybuilder

import (
	"fmt"
)

// ColumnFunctionType is a struct holding column functions
type ColumnFunctionType struct {
	functionName string //round
	functionType string // if to be used only on string /int /time types , example count to be used on anything, avg to be used on int or float type
	// functionParams            []string //supposed round(column name, places), so places will be the param
	FinalColumnFunctionPhrase string //mix of above, with placeholder for column name
}

// BuildColumnFunctionTypeObj is a function to build final phrase for column function
func (cft *ColumnFunctionType) BuildColumnFunctionTypeObj(columnFunctionName string, columnFunctionType string) {
	cft.functionName = columnFunctionName
	cft.functionType = columnFunctionType
	if cft.functionName == "" {
		cft.FinalColumnFunctionPhrase = "%s"
	} else {
		cft.FinalColumnFunctionPhrase = fmt.Sprintf("%s(%s) ", cft.functionName, "%s")
	}
}
