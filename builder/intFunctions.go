package querybuilder

import "fmt"

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
		if qb.SQLQuery.SQLColumnTypes[columnName] == columnType {
			return true
		}
	}
	return false
}

func (qb *Obj) processIntArrayInput(inputCol string, arrayInput []float64, groupNum int) {
	columnItem := ColumnNameStruct{}
	columnFunctionItem := ColumnFunctionType{}
	columnFunctionItem.BuildColumnFunctionTypeObj("", "")
	columnItem.BuildColumnNameStructObj(inputCol, "", "", columnFunctionItem)
	operatorItem := OperatorStruct{}

	for innerIdx, arrayVal := range arrayInput {
		operatorItem.BuildOperatorInt(columnItem, arrayVal, qb.SQLLanguageLiterals.EqualToInt, qb.SQLLanguageLiterals.Language)
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

func (qb *Obj) processIntJSONInput(inputCol string, jsonInput IntJSON, groupNum int) {
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
	if fmt.Sprintf("%v", jsonInput.Gt) != "<nil>" {
		countKeys++
		var gtArray *[]float64
		gtArray = &[]float64{*jsonInput.Gt}
		qb.appendIntOperatorClauseToFinalObj(inputCol, gtArray, qb.SQLLanguageLiterals.Gt, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.Lt) != "<nil>" {
		countKeys++
		var ltArray *[]float64
		ltArray = &[]float64{*jsonInput.Lt}
		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", operatorValue))
			qb.appendIntOperatorClauseToFinalObj(inputCol, ltArray, qb.SQLLanguageLiterals.Lt, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, ltArray, qb.SQLLanguageLiterals.Lt, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.Gte) != "<nil>" {
		countKeys++
		var gtArray *[]float64
		gtArray = &[]float64{*jsonInput.Gte}
		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", operatorValue))
			qb.appendIntOperatorClauseToFinalObj(inputCol, gtArray, qb.SQLLanguageLiterals.Gte, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, gtArray, qb.SQLLanguageLiterals.Gte, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.Lte) != "<nil>" {
		countKeys++
		var gtArray *[]float64
		gtArray = &[]float64{*jsonInput.Lte}
		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", operatorValue))
			qb.appendIntOperatorClauseToFinalObj(inputCol, gtArray, qb.SQLLanguageLiterals.Lte, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, gtArray, qb.SQLLanguageLiterals.Lte, "", groupNum)
	}

	if fmt.Sprintf("%v", jsonInput.Equal) != "<nil>" {
		countKeys++

		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", operatorValue))
			qb.appendIntOperatorClauseToFinalObj(inputCol, jsonInput.Equal, qb.SQLLanguageLiterals.EqualToInt, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, jsonInput.Equal, qb.SQLLanguageLiterals.EqualToInt, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.NotEqual) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", operatorValue))
			qb.appendIntOperatorClauseToFinalObj(inputCol, jsonInput.NotEqual, qb.SQLLanguageLiterals.NotEqualToInt, operatorValue, groupNum)
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], ")")
			return
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, jsonInput.NotEqual, qb.SQLLanguageLiterals.NotEqualToInt, "", groupNum)
	}
	return
}

func (qb *Obj) appendIntOperatorClauseToFinalObj(inputCol string, jsonStringList *[]float64, jsonKeyLiteral string, operatorVal string, groupNum int) {
	columnItem := ColumnNameStruct{}
	columnFunctionItem := ColumnFunctionType{}
	columnFunctionItem.BuildColumnFunctionTypeObj("", "")
	columnItem.BuildColumnNameStructObj(inputCol, "", "", columnFunctionItem)
	operatorItem := OperatorStruct{}

	for innerIdx, arrayVal := range *jsonStringList {
		operatorItem = OperatorStruct{}
		operatorItem.BuildOperatorInt(columnItem, arrayVal, jsonKeyLiteral, qb.SQLLanguageLiterals.Language)
		if innerIdx == 0 { //first key, but not last
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], "( "+operatorItem.FinalOperatorPhrase)
		} else {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
		}
	}
}

func (qb *Obj) appendStringOperatorClauseToFinalObj(inputCol string, jsonStringList *[]string, jsonKeyLiteral string, operatorVal string, groupNum int) {
	columnItem := ColumnNameStruct{}
	columnFunctionItem := ColumnFunctionType{}
	columnFunctionItem.BuildColumnFunctionTypeObj("", "")
	columnItem.BuildColumnNameStructObj(inputCol, "", "", columnFunctionItem)
	var operatorItem OperatorStruct
	for innerIdx, arrayVal := range *jsonStringList {
		operatorItem = OperatorStruct{}
		operatorItem.BuildOperatorString(columnItem, arrayVal, jsonKeyLiteral, qb.SQLLanguageLiterals.Language)
		if innerIdx == 0 { //first key, but not last
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], "( "+operatorItem.FinalOperatorPhrase)
		} else {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
		}
	}
}
