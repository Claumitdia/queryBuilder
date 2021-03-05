package querybuilder

import (
	"fmt"
	"log"
)

//StringJSON is a format for the json input allowed for string columns in the URL
type StringJSON struct {
	StartsWith        *[]string `json:"startsWith"`
	DoesNotStartsWith *[]string `json:"doesNotStartsWith"`
	Contains          *[]string `json:"contains"`
	DoesNotContains   *[]string `json:"doesNotContains"`
	EndsWith          *[]string `json:"endsWith"`
	DoesNotEndsWith   *[]string `json:"doesNotEndsWith"`
	Equal             *[]string `json:"equal"`
	NotEqual          *[]string `json:"notEqual"`
	Operator          *string   `json:"operator"`
}

func (qb *Obj) columnIsString(columnName string) bool {
	for _, columnType := range qb.SQLLanguageLiterals.StringType {
		if qb.SQLQuery.SQLColumnTypes[columnName] == columnType {
			return true
		}
	}
	return false
}

func (qb *Obj) processStringJSONInput(inputCol string, jsonInput StringJSON, groupNum int, having bool) {
	var operatorValue string
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
	if fmt.Sprintf("%v", jsonInput.Contains) != "<nil>" {
		countKeys++
		var op string

		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.Contains, qb.SQLLanguageLiterals.Contains, groupNum, having, op)
	}
	if fmt.Sprintf("%v", jsonInput.DoesNotContains) != "<nil>" {
		countKeys++
		var op string

		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotContains, qb.SQLLanguageLiterals.DoesNotContain, groupNum, having, op)
		if countKeys == 2 {
			return
		}
	}
	if fmt.Sprintf("%v", jsonInput.StartsWith) != "<nil>" {
		countKeys++
		var op string

		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}

		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.StartsWith, qb.SQLLanguageLiterals.StartsWith, groupNum, having, op)

		if countKeys == 2 {
			return
		}
	}
	if fmt.Sprintf("%v", jsonInput.DoesNotStartsWith) != "<nil>" {
		countKeys++
		var op string

		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotStartsWith, qb.SQLLanguageLiterals.DoesNotStartWith, groupNum, having, op)
		if countKeys == 2 {
			return
		}
	}
	if fmt.Sprintf("%v", jsonInput.EndsWith) != "<nil>" {
		countKeys++
		var op string

		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.EndsWith, qb.SQLLanguageLiterals.EndsWith, groupNum, having, op)
		if countKeys == 2 {
			return
		}
	}
	if fmt.Sprintf("%v", jsonInput.DoesNotEndsWith) != "<nil>" {
		countKeys++
		var op string

		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotEndsWith, qb.SQLLanguageLiterals.DoesNotEndWith, groupNum, having, op)
		if countKeys == 2 {
			return
		}
	}
	if fmt.Sprintf("%v", jsonInput.Equal) != "<nil>" {
		countKeys++
		var op string

		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.Equal, qb.SQLLanguageLiterals.EqualToString, groupNum, having, op)
		if countKeys == 2 {
			return
		}
	}
	if fmt.Sprintf("%v", jsonInput.NotEqual) != "<nil>" {
		countKeys++
		var op string

		if countKeys == 2 {
			op = operatorValue
		} else {
			op = ""
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.NotEqual, qb.SQLLanguageLiterals.NotEqualToString, groupNum, having, op)
		if countKeys == 2 {
			return
		}
	}
	return
}

func (qb *Obj) processStringArrayInput(inputCol string, arrayInput []string, groupNum int, having bool) {
	columnItem := ColumnNameStruct{}
	columnFunctionItem := ColumnFunctionType{}
	columnFunctionItem.BuildColumnFunctionTypeObj("", "")
	columnItem.BuildColumnNameStructObj(inputCol, "", "", columnFunctionItem)
	operatorItem := OperatorStruct{}
	log.Println("here in")
	if having {
		qb.SQLQuery.HavingPhrase[groupNum] = append(qb.SQLQuery.HavingPhrase[groupNum], "(")
	} else {
		qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], "(")
	}
	log.Println(qb.SQLQuery.OperatorPhrase[groupNum])
	for innerIdx, arrayVal := range arrayInput {
		operatorItem = OperatorStruct{}
		operatorItem.BuildOperatorString(columnItem, arrayVal, qb.SQLLanguageLiterals.Contains, qb.SQLLanguageLiterals.Language)
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

func (qb *Obj) appendStringOperatorClauseToFinalObj(inputCol string, jsonStringList *[]string, jsonKeyLiteral string, groupNum int, having bool, op string) {
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
		operatorItem.BuildOperatorString(columnItem, arrayVal, jsonKeyLiteral, qb.SQLLanguageLiterals.Language)
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
