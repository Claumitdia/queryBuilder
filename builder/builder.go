package querybuilder

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strconv"
	s "strings"
	"time"
)

// QueryBuilder is an interface for querybuilder
type QueryBuilder interface {
	SQLBuilderFromURL(url.Values)
	QueryBuilderFunc() (string, error)
	calculateStartEndTime(url.Values)
}

//QueryBuilderFunc - joins all phrases of QueryBuilder.SQLQuery to form final query string
func (qb *Obj) QueryBuilderFunc() (string, error) {
	var finalQuery string
	if qb.SQLQuery.SelectPhrase.FinalSelectPhrase != "" {
		finalQuery = qb.SQLQuery.SelectPhrase.FinalSelectPhrase
		if qb.SQLQuery.FromPhrase.FinalFromPhrase != "" {
			finalQuery += qb.SQLQuery.FromPhrase.FinalFromPhrase
			if qb.SQLQuery.WherePhrase.FinalWherePhrase != "" {
				finalQuery += qb.SQLQuery.WherePhrase.FinalWherePhrase
				//for and and or
				for _, opPhrase := range qb.SQLQuery.OperatorPhrase {
					for _, o := range opPhrase {
						finalQuery += o
					}
				}
				if qb.SQLQuery.GroupByPhrase.FinalGroupByPhrase != "" {
					finalQuery += qb.SQLQuery.GroupByPhrase.FinalGroupByPhrase
					if qb.SQLQuery.HavingPhrase.FinalHavingPhrase != "" {
						finalQuery += qb.SQLQuery.HavingPhrase.FinalHavingPhrase
					}
				}
				if qb.SQLQuery.OrderByPhrase.FinalOrderByPhrase != "" {
					finalQuery += qb.SQLQuery.OrderByPhrase.FinalOrderByPhrase
				}

			}
			if qb.SQLQuery.LimitPhrase.FinalLimitPhrase != "" {
				finalQuery += qb.SQLQuery.LimitPhrase.FinalLimitPhrase
			}
			return finalQuery, nil
		}
		return "", errors.New("ErrorQueryBuilder: No from clause")
	}
	return "", errors.New("ErrorQueryBuilder: No Query")
}

//SQLBuilderFromURL - fills QueryBuilder.SQLQuery with all phrases
func (qb *Obj) SQLBuilderFromURL(queryParametersURLValues url.Values) {
	log.Println(qb.SQLLanguageLiterals.Language)
	if len(queryParametersURLValues["endTime"]) == 0 {
		// log.Println("inside if of endtime")
		qb.calculateStartEndTime(queryParametersURLValues["startTime"][0], time.Now().Local().Format("2006-01-02 15:04:05"))
	} else {
		// log.Println("inside else of endtime")
		qb.calculateStartEndTime(queryParametersURLValues["startTime"][0], queryParametersURLValues["endTime"][0])
	}

	//since already considered we can delete, will be present in sql query object of qb
	delete(queryParametersURLValues, "endTime")

	for key, val := range queryParametersURLValues {
		if key == "column" {
			// log.Println("inside column key")
			selectColumnList := s.Split(val[0], ",")
			var colName string
			var colFunc string
			var colAlias string

			var selectColumnNameObjList []ColumnNameStruct
			for _, c := range selectColumnList {

				if s.Index(c, ".") != -1 {
					colName = c[:s.Index(c, ".")]
					colFunc = c[(s.Index(c, ".") + 1):]
					colAlias = c
				} else {
					colName = c
					colFunc = ""
					colAlias = ""
				}

				columnNameObj := ColumnNameStruct{}
				cft := ColumnFunctionType{}

				cft.BuildColumnFunctionTypeObj(colFunc, "")
				columnNameObj.BuildColumnNameStructObj(colName, "", colAlias, cft)
				selectColumnNameObjList = append(selectColumnNameObjList, columnNameObj)
			}
			qb.BuildSelect(selectColumnNameObjList, qb.SQLLanguageLiterals.SelectKeyword)
		} else if key == "limit" {
			limitVal, limitValErr := strconv.Atoi(val[0])
			if limitValErr != nil {
				panic(limitValErr)
			}
			qb.BuildLimit(limitVal, qb.SQLLanguageLiterals.LimitKeyWord)
		} else if key == "startTime" {
			//where
			var opObjStartime OperatorStruct
			var opObjEndTime OperatorStruct
			columnNameObj := ColumnNameStruct{}
			cft := ColumnFunctionType{}
			cft.BuildColumnFunctionTypeObj("", "")
			columnNameObj.BuildColumnNameStructObj(qb.SQLLanguageLiterals.TimeFieldName, "timestamp", "", cft)
			opObjStartime.BuildOperatorString(columnNameObj, qb.SQLQuery.StartTime, qb.SQLLanguageLiterals.Gte, qb.SQLLanguageLiterals.Language)
			qb.BuildWhere(&opObjStartime, qb.SQLLanguageLiterals.WhereKeyword)
			opObjEndTime.BuildOperatorString(columnNameObj, qb.SQLQuery.EndTime, qb.SQLLanguageLiterals.Lte, qb.SQLLanguageLiterals.Language)
			qb.SQLQuery.OperatorPhrase = map[int][]string{
				100: {fmt.Sprintf("%s ", qb.SQLLanguageLiterals.AndKeyword) + opObjEndTime.FinalOperatorPhrase},
			}
		} else if key == "dataSource" {
			log.Println("table name: ", val[0])
			qb.SQLQuery.SQLTableName = val[0]
			qb.BuildFrom()
		}
	}

	delete(queryParametersURLValues, "startTime")
	delete(queryParametersURLValues, "limit")
	delete(queryParametersURLValues, "dataSource")

	log.Println("\ncolumn dt map: ", qb.SQLQuery.SQLColumnTypes)
	for key, val := range queryParametersURLValues {
		if key != "column" {
			log.Println("key value:", key)
			log.Println("val value:", val)
			for intMap, singleVal := range val {
				if string(singleVal[0]) == "{" && string(singleVal[len(singleVal)-1]) == "}" {
					log.Println("singlevalue:", singleVal)
					//process json
					if qb.columnIsString(key) {
						log.Println("is string true")
						var typeStruct StringJSON
						_ = json.Unmarshal([]byte(singleVal), &typeStruct)
						qb.SQLQuery.OperatorPhrase[intMap] = []string{}
						qb.processStringJSONInput(key, typeStruct, intMap)
					} else if qb.columnIsInt(key) {
						log.Println("is int true")
						var typeStruct IntJSON
						_ = json.Unmarshal([]byte(singleVal), &typeStruct)
						qb.SQLQuery.OperatorPhrase[intMap] = []string{}
						qb.processIntJSONInput(key, typeStruct, intMap)
					}
				} else {
					//process array
					log.Println("inside array")
					stringArray := s.Split(singleVal, ",")
					if qb.columnIsInt(key) {
						var intArrayInput []float64
						for _, vali := range stringArray {
							j, jerr := strconv.ParseFloat(vali, 64)
							if jerr != nil {
								log.Println(jerr)
							}
							intArrayInput = append(intArrayInput, j)
						}
						qb.SQLQuery.OperatorPhrase[intMap] = []string{}
						qb.processIntArrayInput(key, intArrayInput, intMap)
					} else if qb.columnIsString(key) {
						log.Println("inside string array")
						qb.SQLQuery.OperatorPhrase[intMap] = []string{}
						qb.processStringArrayInput(key, stringArray, intMap)
					}
				}
			}
		}
	}

}

//calculateStartEndTime - calculates start and endtime and stores in QueryBuilder.SQLQuery.startTime and QueryBuilder.SQLQuery.endTime
func (qb *Obj) calculateStartEndTime(startTime string, endTime string) {
	var startTimeStr string
	var endTimeStr string
	var endTimeT time.Time
	var startTimeT time.Time
	endTimeT, endTimeTErr := time.Parse("2006-01-02 15:04:05", endTime)
	if endTimeTErr != nil {
		endTimeT, _ = time.Parse("2006-01-02 15:04:05", endTime+" 00:00:00")
	}
	if s.Contains(startTime, "-ago") {
		userDuration := startTime[:s.Index(startTime, "-ago")]
		userTimeSpanChar := string(userDuration[len(userDuration)-1])
		userTimeSpanInt, _ := strconv.Atoi(userDuration[:len(userDuration)-1])
		switch string(userTimeSpanChar) {
		case "s":
			startTimeT = endTimeT.Add(time.Second * time.Duration((-1 * userTimeSpanInt)))
		case "m":
			startTimeT = endTimeT.Add(time.Minute * time.Duration((-1 * userTimeSpanInt)))
		case "h":
			startTimeT = endTimeT.Add(time.Hour * time.Duration((-1 * userTimeSpanInt)))
		case "D":
			startTimeT = endTimeT.AddDate(0, 0, -userTimeSpanInt)
		case "M":
			startTimeT = endTimeT.AddDate(0, -userTimeSpanInt, 0)
		case "Y":
			startTimeT = endTimeT.AddDate(-userTimeSpanInt, 0, 0)
		}
	} else {
		startTimeT, endTimeTErr = time.Parse("2006-01-02 15:04:05", startTime)
		if endTimeTErr != nil {
			startTimeT, _ = time.Parse("2006-01-02 15:04:05", startTime+" 00:00:00")
		}
	}
	startTimeStr = startTimeT.String()
	endTimeStr = endTimeT.String()
	qb.SQLQuery.StartTime = startTimeStr[:19]
	qb.SQLQuery.EndTime = endTimeStr[:19]
}

//Obj is a struct of type QueryBuilderInterface, which is an object representing a db query builder, example druidObj will have its own literals and a query associated.
type Obj struct {
	SQLLanguageLiterals SQLLanguageLiterals
	SQLQuery            SQLQueryObj
}

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
	log.Println("inside columnIsInt")
	for _, columnType := range qb.SQLLanguageLiterals.NumberType {
		if qb.SQLQuery.SQLColumnTypes[columnName] == columnType {
			return true
		}
	}
	return false
}

func (qb *Obj) columnIsString(columnName string) bool {
	log.Println("inside columnIsString")
	for _, columnType := range qb.SQLLanguageLiterals.StringType {
		if qb.SQLQuery.SQLColumnTypes[columnName] == columnType {
			return true
		}
	}
	return false
}

func (qb *Obj) processStringJSONInput(inputCol string, jsonInput StringJSON, groupNum int) {
	log.Println("inside process String json")
	var operatorValue string
	if fmt.Sprintf("%v", jsonInput.Operator) != "<nil>" {
		log.Println(jsonInput.Operator)
		operatorValue = *jsonInput.Operator
	} else {
		operatorValue = "and"
	}

	log.Println("operatorValue: ", operatorValue)

	countKeys := 0
	if fmt.Sprintf("%v", jsonInput.Contains) != "<nil>" {
		countKeys++
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.Contains, qb.SQLLanguageLiterals.Contains, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.DoesNotContains) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotContains, qb.SQLLanguageLiterals.DoesNotContain, operatorValue, groupNum)
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotContains, qb.SQLLanguageLiterals.DoesNotContain, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.StartsWith) != "<nil>" {

		countKeys++
		log.Println("countKeys: ", countKeys)
		if countKeys == 2 {
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.StartsWith, qb.SQLLanguageLiterals.StartsWith, operatorValue, groupNum)
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.StartsWith, qb.SQLLanguageLiterals.StartsWith, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.DoesNotStartsWith) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotStartsWith, qb.SQLLanguageLiterals.DoesNotStartWith, operatorValue, groupNum)
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotStartsWith, qb.SQLLanguageLiterals.DoesNotStartWith, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.EndsWith) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.EndsWith, qb.SQLLanguageLiterals.EndsWith, operatorValue, groupNum)
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.EndsWith, qb.SQLLanguageLiterals.EndsWith, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.DoesNotEndsWith) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotEndsWith, qb.SQLLanguageLiterals.DoesNotEndWith, operatorValue, groupNum)
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.DoesNotEndsWith, qb.SQLLanguageLiterals.DoesNotEndWith, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.Equal) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.Equal, qb.SQLLanguageLiterals.EqualToString, operatorValue, groupNum)
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.Equal, qb.SQLLanguageLiterals.EqualToString, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.NotEqual) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.NotEqual, qb.SQLLanguageLiterals.NotEqualToString, operatorValue, groupNum)
			return
		}
		qb.appendStringOperatorClauseToFinalObj(inputCol, jsonInput.NotEqual, qb.SQLLanguageLiterals.NotEqualToString, "", groupNum)
	}
	return
}

func (qb *Obj) processIntJSONInput(inputCol string, jsonInput IntJSON, groupNum int) {
	log.Println("inside process Int json")
	var operatorValue string
	if fmt.Sprintf("%v", jsonInput.Operator) != "<nil>" {
		log.Println(jsonInput.Operator)
		operatorValue = *jsonInput.Operator
	} else {
		operatorValue = "and"
	}

	log.Println("operatorValue: ", operatorValue)

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
			qb.appendIntOperatorClauseToFinalObj(inputCol, ltArray, qb.SQLLanguageLiterals.Lt, operatorValue, groupNum)
			return
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, ltArray, qb.SQLLanguageLiterals.Lt, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.Gte) != "<nil>" {
		countKeys++
		var gtArray *[]float64
		gtArray = &[]float64{*jsonInput.Gte}
		if countKeys == 2 {
			qb.appendIntOperatorClauseToFinalObj(inputCol, gtArray, qb.SQLLanguageLiterals.Gte, operatorValue, groupNum)
			return
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, gtArray, qb.SQLLanguageLiterals.Gte, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.Lte) != "<nil>" {
		countKeys++
		var gtArray *[]float64
		gtArray = &[]float64{*jsonInput.Lte}
		if countKeys == 2 {
			qb.appendIntOperatorClauseToFinalObj(inputCol, gtArray, qb.SQLLanguageLiterals.Lte, operatorValue, groupNum)
			return
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, gtArray, qb.SQLLanguageLiterals.Lte, "", groupNum)
	}

	if fmt.Sprintf("%v", jsonInput.Equal) != "<nil>" {
		countKeys++

		if countKeys == 2 {
			qb.appendIntOperatorClauseToFinalObj(inputCol, jsonInput.Equal, qb.SQLLanguageLiterals.EqualToInt, operatorValue, groupNum)
			return
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, jsonInput.Equal, qb.SQLLanguageLiterals.EqualToInt, "", groupNum)
	}
	if fmt.Sprintf("%v", jsonInput.NotEqual) != "<nil>" {
		countKeys++
		if countKeys == 2 {
			qb.appendIntOperatorClauseToFinalObj(inputCol, jsonInput.NotEqual, qb.SQLLanguageLiterals.NotEqualToInt, operatorValue, groupNum)
			return
		}
		qb.appendIntOperatorClauseToFinalObj(inputCol, jsonInput.NotEqual, qb.SQLLanguageLiterals.NotEqualToInt, "", groupNum)
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
			log.Println("first key")
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", qb.SQLLanguageLiterals.AndKeyword)+operatorItem.FinalOperatorPhrase)
		} else if innerIdx == 0 && innerIdx == len(arrayInput)-1 { //first but last
			log.Println("only one key")
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.AndKeyword)+operatorItem.FinalOperatorPhrase)
		} else if innerIdx == len(arrayInput)-1 && innerIdx != 0 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase+")")
		} else {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
		}
	}
}

func (qb *Obj) processIntArrayInput(inputCol string, arrayInput []float64, groupNum int) {
	columnItem := ColumnNameStruct{}
	columnFunctionItem := ColumnFunctionType{}
	columnFunctionItem.BuildColumnFunctionTypeObj("", "")
	columnItem.BuildColumnNameStructObj(inputCol, "", "", columnFunctionItem)
	operatorItem := OperatorStruct{}

	for innerIdx, arrayVal := range arrayInput {
		log.Println("value: ", arrayVal)
		operatorItem.BuildOperatorInt(columnItem, arrayVal, qb.SQLLanguageLiterals.EqualToInt, qb.SQLLanguageLiterals.Language)
		if innerIdx == 0 && innerIdx != len(arrayInput)-1 { //first key, but not last
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", qb.SQLLanguageLiterals.AndKeyword)+operatorItem.FinalOperatorPhrase)
		} else if innerIdx == 0 && innerIdx == len(arrayInput)-1 { //first but last
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.AndKeyword)+operatorItem.FinalOperatorPhrase)
		} else if innerIdx == len(arrayInput)-1 && innerIdx != 0 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase+")")
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
	operatorItem := OperatorStruct{}

	for innerIdx, arrayVal := range *jsonStringList {
		operatorItem.BuildOperatorString(columnItem, arrayVal, jsonKeyLiteral, qb.SQLLanguageLiterals.Language)
		log.Println(arrayVal)
		if innerIdx == 0 && innerIdx != len(*jsonStringList)-1 { //first key, but not last
			log.Println("first key")
			if operatorVal == "and" || operatorVal == "" {
				log.Println("inside and first key")
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", qb.SQLLanguageLiterals.AndKeyword)+operatorItem.FinalOperatorPhrase)
			} else {
				log.Println("inside or first key")
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
			}
		} else if innerIdx == 0 && innerIdx == len(*jsonStringList)-1 { //first but last
			log.Println("only one key")
			if operatorVal == "and" || operatorVal == "" {
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.AndKeyword)+operatorItem.FinalOperatorPhrase)
			} else {
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
			}
		} else if innerIdx == len(*jsonStringList)-1 && innerIdx != 0 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase+")")
		} else {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
		}
	}
	log.Println(qb.SQLQuery.OperatorPhrase)
}

func (qb *Obj) appendIntOperatorClauseToFinalObj(inputCol string, jsonStringList *[]float64, jsonKeyLiteral string, operatorVal string, groupNum int) {
	columnItem := ColumnNameStruct{}
	columnFunctionItem := ColumnFunctionType{}
	columnFunctionItem.BuildColumnFunctionTypeObj("", "")
	columnItem.BuildColumnNameStructObj(inputCol, "", "", columnFunctionItem)
	operatorItem := OperatorStruct{}

	for innerIdx, arrayVal := range *jsonStringList {
		operatorItem.BuildOperatorInt(columnItem, arrayVal, jsonKeyLiteral, qb.SQLLanguageLiterals.Language)
		log.Println(arrayVal)
		if innerIdx == 0 && innerIdx != len(*jsonStringList)-1 { //first key, but not last
			log.Println("first key")
			if operatorVal == "and" || operatorVal == "" {
				log.Println("inside and first key")
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", qb.SQLLanguageLiterals.AndKeyword)+operatorItem.FinalOperatorPhrase)
			} else {
				log.Println("inside or first key")
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s (", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
			}
		} else if innerIdx == 0 && innerIdx == len(*jsonStringList)-1 { //first but last
			log.Println("only one key")
			if operatorVal == "and" || operatorVal == "" {
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.AndKeyword)+operatorItem.FinalOperatorPhrase)
			} else {
				qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
			}
		} else if innerIdx == len(*jsonStringList)-1 && innerIdx != 0 {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase+")")
		} else {
			qb.SQLQuery.OperatorPhrase[groupNum] = append(qb.SQLQuery.OperatorPhrase[groupNum], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.OrKeyword)+operatorItem.FinalOperatorPhrase)
		}
	}
	log.Println(qb.SQLQuery.OperatorPhrase)
}
