package querybuilder

import (
	"fmt"
)

// IntJSON is a format for the json input
type IntJSON struct {
	Gt       *float64   `json:"gt"`
	Lt       *float64   `json:"lt"`
	Gte      *float64   `json:"gte"`
	Lte      *float64   `json:"lte"`
	Equal    *[]float64 `json:"equal"`
	NotEqual *[]float64 `json:"notEqual"`
	Operator *string    `json:"operator"`
}

func (qb *Obj) columnIsInt(columnName string) bool {
	for _, columnType := range qb.SQLLanguageLiterals.NumberType {
		// log.Println("column type: ", columnType)
		// log.Println("qb.SQLQuery.SQLColumnTypes[columnName]:", qb.SQLQuery.SQLColumnTypes[columnName])
		if qb.SQLQuery.SQLColumnTypes[columnName] == columnType {
			return true
		}
	}
	return false
}

func (qb *Obj) processIntArrayInput(inputCol string, arrayInput []float64, groupNum int, having bool) {
	columnItem := ColumnNameStruct{}
	columnFunctionItem := ColumnFunctionType{}
	columnFunctionItem.BuildColumnFunctionTypeObj("", "")
	columnItem.BuildColumnNameStructObj(inputCol, "", "", columnFunctionItem)
	operatorItem := OperatorStruct{}

	if having {
		qb.SQLQuery.HavingPhrase[groupNum] = append(qb.SQLQuery.HavingPhrase[groupNum], "(")
	} else {
		qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], "(")
	}

	for innerIdx, arrayVal := range arrayInput {
		operatorItem = OperatorStruct{}
		operatorItem.BuildOperatorInt(columnItem, arrayVal, qb.SQLLanguageLiterals.EqualToInt, qb.SQLLanguageLiterals.Language)
		if innerIdx == 0 { //first key, but not last
			if having {
				qb.SQLQuery.HavingPhrase[groupNum] = append(qb.SQLQuery.HavingPhrase[groupNum], operatorItem.FinalOperatorPhrase)
			} else {
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], operatorItem.FinalOperatorPhrase)
			}
		} else {
			if having {
				qb.SQLQuery.HavingPhrase[groupNum] = append(qb.SQLQuery.HavingPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
			} else {
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
			}
		}
	}

	if having {
		qb.SQLQuery.HavingPhrase[groupNum] = append(qb.SQLQuery.HavingPhrase[groupNum], ") ")
	} else {
		qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ") ")
	}
}

func (qb *Obj) processIntJSONInput(inputCol string, jsonInput IntJSON, groupNum int, having bool) {
	var operatorValue string
	// log.Println("inside processIntJSONInput")
	if fmt.Sprintf("%v", jsonInput.Operator) != "<nil>" {
		operatorValue = *jsonInput.Operator
		if operatorValue == "and" || operatorValue == "AND" {
			operatorValue = qb.SQLLanguageLiterals.AndKeyword
		} else if operatorValue == "or" || operatorValue == "OR" {
			operatorValue = qb.SQLLanguageLiterals.OrKeyword
		}
	} else {
		operatorValue = qb.SQLLanguageLiterals.AndKeyword
	}

	countKeys := 0
	// log.Println("inside processIntJSONInput")
	if fmt.Sprintf("%v", jsonInput.Gt) != "<nil>" {
		countKeys++
		var op string

		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		var gtArray *[]float64
		gtArray = &[]float64{*jsonInput.Gt}
		qb.appendIntOperatorClauseToFinalObj(inputCol, gtArray, qb.SQLLanguageLiterals.Gt, groupNum, having, op)
	}

	// log.Println("inside processIntJSONInput")
	if fmt.Sprintf("%v", jsonInput.Lt) != "<nil>" {
		countKeys++
		var ltArray *[]float64
		ltArray = &[]float64{*jsonInput.Lt}
		var op string
		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, ltArray, qb.SQLLanguageLiterals.Lt, groupNum, having, op)
		if countKeys == 2 {
			return
		}
	}
	if fmt.Sprintf("%v", jsonInput.Gte) != "<nil>" {
		// log.Println("inside GTE")
		countKeys++
		var ltArray *[]float64
		ltArray = &[]float64{*jsonInput.Gte}
		// log.Println("ltArray: ", ltArray)
		var op string
		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		// log.Println("reached here")
		qb.appendIntOperatorClauseToFinalObj(inputCol, ltArray, qb.SQLLanguageLiterals.Gte, groupNum, having, op)
		if countKeys == 2 {
			return
		}
	}
	if fmt.Sprintf("%v", jsonInput.Lte) != "<nil>" {
		countKeys++
		var ltArray *[]float64
		ltArray = &[]float64{*jsonInput.Lte}
		var op string
		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, ltArray, qb.SQLLanguageLiterals.Lte, groupNum, having, op)
		if countKeys == 2 {
			return
		}
	}

	if fmt.Sprintf("%v", jsonInput.Equal) != "<nil>" {
		countKeys++
		var ltArray *[]float64
		ltArray = &*jsonInput.Equal
		var op string
		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, ltArray, qb.SQLLanguageLiterals.EqualToInt, groupNum, having, op)
		if countKeys == 2 {
			return
		}
	}
	if fmt.Sprintf("%v", jsonInput.NotEqual) != "<nil>" {
		countKeys++
		var ltArray *[]float64
		ltArray = &*jsonInput.NotEqual
		var op string
		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, ltArray, qb.SQLLanguageLiterals.NotEqualToInt, groupNum, having, op)
		if countKeys == 2 {
			return
		}
	}
	return
}

func (qb *Obj) appendIntOperatorClauseToFinalObj(inputCol string, jsonStringList *[]float64, jsonKeyLiteral string, groupNum int, having bool, op string) {
	columnItem := ColumnNameStruct{}
	columnFunctionItem := ColumnFunctionType{}
	columnFunctionItem.BuildColumnFunctionTypeObj("", "")
	columnItem.BuildColumnNameStructObj(inputCol, "", "", columnFunctionItem)
	var operatorItem OperatorStruct

	if having {
		qb.SQLQuery.HavingPhrase[groupNum] = append(qb.SQLQuery.HavingPhrase[groupNum], fmt.Sprintf("%s(", op))
	} else {
		qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf("%s(", op))
	}

	for innerIdx, arrayVal := range *jsonStringList {
		operatorItem = OperatorStruct{}
		operatorItem.BuildOperatorInt(columnItem, arrayVal, jsonKeyLiteral, qb.SQLLanguageLiterals.Language)
		if innerIdx == 0 { //first key, but not last
			if having {
				qb.SQLQuery.HavingPhrase[groupNum] = append(qb.SQLQuery.HavingPhrase[groupNum], operatorItem.FinalOperatorPhrase)
			} else {
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], operatorItem.FinalOperatorPhrase)
			}
		} else {
			if having {
				qb.SQLQuery.HavingPhrase[groupNum] = append(qb.SQLQuery.HavingPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
			} else {
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
			}
		}
	}

	if having {
		qb.SQLQuery.HavingPhrase[groupNum] = append(qb.SQLQuery.HavingPhrase[groupNum], ") ")
	} else {
		qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ") ")
	}
}
