package querybuilder

import (
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	s "strings"
	"time"
)

// QueryBuilder is an interface for querybuilder
type QueryBuilder interface {
	SQLBuilderFromURL(url.Values)
	QueryBuilderFunc() (string, error)
	processInputValue(string)
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
				if len(qb.SQLQuery.AndPhrase) != 0 {
					for _, andPhrase := range qb.SQLQuery.AndPhrase {
						finalQuery += s.Replace(andPhrase.FinalAndPhrase, "AND", "AND (", 1)
						for _, orPhrase := range qb.SQLQuery.OrPhrase {
							if orPhrase.GroupNum == andPhrase.GroupNum {
								finalQuery += orPhrase.FinalOrPhrase
							}
						}
						finalQuery += ")"
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
	qb.SQLQuery = SQLQueryObj{}

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
			opObjStartime.BuildOperator(columnNameObj, qb.SQLQuery.StartTime, qb.SQLLanguageLiterals.Gte, qb.SQLLanguageLiterals.Language)
			qb.BuildWhere(&opObjStartime, qb.SQLLanguageLiterals.WhereKeyword)
			opObjEndTime.BuildOperator(columnNameObj, qb.SQLQuery.EndTime, qb.SQLLanguageLiterals.Lte, qb.SQLLanguageLiterals.Language)
			qb.SQLQuery.AndPhrase = append(qb.SQLQuery.AndPhrase, AndStruct{
				AndKeyword:  qb.SQLLanguageLiterals.AndKeyword,
				AndOperator: opObjEndTime,
			})
		} else if key == "dataSource" {
			qb.BuildFrom(val[0], qb.SQLLanguageLiterals.FromKeyword)
		}
	}

	//this will add entime stmt
	qb.BuildAnd()

	delete(queryParametersURLValues, "startTime")
	delete(queryParametersURLValues, "limit")
	delete(queryParametersURLValues, "dataSource")

	for key, val := range queryParametersURLValues {
		if key != "column" {
			qb.processStringInputValue(key, val)
		}
	}

	// log.Println("finalQueryobj :", qb.SQLQuery)
	//build group by columns and 'by' after all done
	//build having also after all done
	//decide which and the column will be ? normal and or with having, samewith or

}

//processStringInputValue will process each columns inputs given in url
//example url willhave : hostName={com,.com,something},{"doesNotContains":"e","operator":"or"}&hostName={}
//url.Values will have hostName=[{com,.com,something},{"doesNotContains":"e","operator":"or"},{}]
func (qb *Obj) processStringInputValue(inputCol string, inputVal []string) {
	groupNum := 0
	for _, singleJSONItem := range inputVal {
		//unmarshall the json if it is one
		// log.Println("singleJSONItem: ", singleJSONItem)
		var unmarshalledJSON StringJSON
		unmarshallJSONErr := json.Unmarshal([]byte(singleJSONItem), &unmarshalledJSON)
		var stringValueList []string
		var literalString string

		if unmarshallJSONErr != nil {
			literalString = qb.SQLLanguageLiterals.Contains
			stringValueList = s.Split(singleJSONItem, ",")
			groupNum++
			qb.processEachJSON(stringValueList, inputCol, literalString, groupNum)
		} else {
			if len(unmarshalledJSON.Contains) != 0 {
				stringValueList = unmarshalledJSON.Contains
				literalString = qb.SQLLanguageLiterals.Contains
				groupNum++
				qb.processEachJSON(stringValueList, inputCol, literalString, groupNum)
			}
			if len(unmarshalledJSON.DoesNotContains) != 0 {
				stringValueList = unmarshalledJSON.DoesNotContains
				literalString = qb.SQLLanguageLiterals.DoesNotContain
				groupNum++
				qb.processEachJSON(stringValueList, inputCol, literalString, groupNum)
			}
			if len(unmarshalledJSON.DoesNotEndsWith) != 0 {
				stringValueList = unmarshalledJSON.DoesNotEndsWith
				literalString = qb.SQLLanguageLiterals.DoesNotEndWith
				groupNum++
				qb.processEachJSON(stringValueList, inputCol, literalString, groupNum)
			}
			if len(unmarshalledJSON.EndsWith) != 0 {
				stringValueList = unmarshalledJSON.EndsWith
				literalString = qb.SQLLanguageLiterals.EndsWith
				groupNum++
				qb.processEachJSON(stringValueList, inputCol, literalString, groupNum)
			}
			if len(unmarshalledJSON.StartsWith) != 0 {
				stringValueList = unmarshalledJSON.StartsWith
				literalString = qb.SQLLanguageLiterals.StartsWith
				groupNum++
				qb.processEachJSON(stringValueList, inputCol, literalString, groupNum)
			}
			if len(unmarshalledJSON.DoesNotStartsWith) != 0 {
				stringValueList = unmarshalledJSON.DoesNotStartsWith
				literalString = qb.SQLLanguageLiterals.DoesNotStartWith
				groupNum++
				qb.processEachJSON(stringValueList, inputCol, literalString, groupNum)
			}
			if len(unmarshalledJSON.Equal) != 0 {
				stringValueList = unmarshalledJSON.Equal
				literalString = qb.SQLLanguageLiterals.InList
				groupNum++
				qb.processEachJSON(stringValueList, inputCol, literalString, groupNum)
			}
			if len(unmarshalledJSON.NotEqual) != 0 {
				stringValueList = unmarshalledJSON.NotEqual
				literalString = qb.SQLLanguageLiterals.NotInList
				groupNum++
				qb.processEachJSON(stringValueList, inputCol, literalString, groupNum)
			}
		}
	}
}

func (qb *Obj) processEachJSON(stringValueList []string, inputCol string, literalString string, groupNum int) {
	leftColFunction := ColumnFunctionType{}
	leftColFunction.BuildColumnFunctionTypeObj("", "")
	leftColName := ColumnNameStruct{}
	leftColName.BuildColumnNameStructObj(inputCol, "", "", leftColFunction)
	operatorItem := OperatorStruct{}
	for idx, containsStringItem := range stringValueList {
		var keyWordStart string
		operatorItem.BuildOperator(leftColName, containsStringItem, literalString, qb.SQLLanguageLiterals.Language)
		if idx == 0 {
			qb.SQLQuery.AndPhrase = append(qb.SQLQuery.AndPhrase, AndStruct{
				AndKeyword:  qb.SQLLanguageLiterals.AndKeyword + keyWordStart,
				AndOperator: operatorItem,
				GroupNum:    strconv.Itoa(groupNum) + operatorItem.LeftColumnName.columnName,
			})
		} else {
			qb.SQLQuery.OrPhrase = append(qb.SQLQuery.OrPhrase, OrStruct{
				OrKeyword:  qb.SQLLanguageLiterals.OrKeyword,
				OrOperator: operatorItem,
				GroupNum:   strconv.Itoa(groupNum) + operatorItem.LeftColumnName.columnName,
			})
		}
		qb.BuildAnd()
		qb.BuildOr()
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
	StartsWith        []string `json:"startsWith"`
	DoesNotStartsWith []string `json:"doesNotStartsWith"`
	Contains          []string `json:"contains"`
	DoesNotContains   []string `json:"doesNotContains"`
	EndsWith          []string `json:"endsWith"`
	DoesNotEndsWith   []string `json:"doesNotEndsWith"`
	Equal             []string `json:"equal"`
	NotEqual          []string `json:"notEqual"`
	Operator          []string `json:"operator"`
}
