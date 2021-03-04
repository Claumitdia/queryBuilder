package querybuilder

import "fmt"

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

func (qb *Obj) processStringJSONInput(inputCol string, jsonInput StringJSON, groupNum int) {

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
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.Contains, qb.SQLLanguageLiterals.Contains, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.DoesNotContains) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", operatorValue))
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotContains, qb.SQLLanguageLiterals.DoesNotContain, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotContains, qb.SQLLanguageLiterals.DoesNotContain, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.StartsWith) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", operatorValue))
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.StartsWith, qb.SQLLanguageLiterals.StartsWith, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.StartsWith, qb.SQLLanguageLiterals.StartsWith, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.DoesNotStartsWith) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", operatorValue))
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotStartsWith, qb.SQLLanguageLiterals.DoesNotStartWith, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotStartsWith, qb.SQLLanguageLiterals.DoesNotStartWith, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.EndsWith) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", operatorValue))
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.EndsWith, qb.SQLLanguageLiterals.EndsWith, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.EndsWith, qb.SQLLanguageLiterals.EndsWith, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.DoesNotEndsWith) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", operatorValue))
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotEndsWith, qb.SQLLanguageLiterals.DoesNotEndWith, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotEndsWith, qb.SQLLanguageLiterals.DoesNotEndWith, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.Equal) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", operatorValue))
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.Equal, qb.SQLLanguageLiterals.EqualToString, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.Equal, qb.SQLLanguageLiterals.EqualToString, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.NotEqual) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", operatorValue))
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.NotEqual, qb.SQLLanguageLiterals.NotEqualToString, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.NotEqual, qb.SQLLanguageLiterals.NotEqualToString, "", groupNum)
	}
	return
}

func (qb *Obj) processStringArrayInput(inputCol string, arrayInput []string, groupNum int) {
	columnItem := ColumnNameStruct{}
	columnFunctionItem := ColumnFunctionType{}
	columnFunctionItem.BuildColumnFunctionTypeObj("", "")
	columnItem.BuildColumnNameStructObj(inputCol, "", "", columnFunctionItem)
	operatorItem := OperatorStruct{}

	for innerIdx, arrayVal := range arrayInput {
		operatorItem.BuildOperatorString(columnItem, arrayVal, qb.SQLLanguageLiterals.Contains, qb.SQLLanguageLiterals.Language)
		if innerIdx == 0 && innerIdx != len(arrayInput)-1 { //first key, but not last
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", qb.SQLLanguageLiterals.AndKeyword)+operatorItem.FinalOperatorPhrase)
		} else if innerIdx == 0 && innerIdx == len(arrayInput)-1 { //first but last
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], operatorItem.FinalOperatorPhrase)
		} else if innerIdx == len(arrayInput)-1 && innerIdx != 0 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase+")")
		} else {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
		}
	}
}
