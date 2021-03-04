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

//Obj is a struct of type QueryBuilderInterface, which is an object representing a db query builder, example druidObj will have its own literals and a query associated.
type Obj struct {
	SQLLanguageLiterals SQLLanguageLiterals
	SQLQuery            SQLQueryObj
}

//SQLBuilderFromURL - fills QueryBuilder.SQLQuery with all phrases
func (qb *Obj) SQLBuilderFromURL(queryParametersURLValues url.Values) {
	if len(queryParametersURLValues["endTime"]) == 0 {
		qb.calculateStartEndTime(queryParametersURLValues["startTime"][0], time.Now().Local().Format("2006-01-02 15:04:05"))
	} else {
		qb.calculateStartEndTime(queryParametersURLValues["startTime"][0], queryParametersURLValues["endTime"][0])
	}

	//since already considered we can delete, will be present in sql query object of qb
	delete(queryParametersURLValues, "endTime")
	var selectColumnNameObjList []ColumnNameStruct
	var groupByColumnNameObjList []ColumnNameStruct
	groupByNeed := false

	for key, val := range queryParametersURLValues {
		if key == "by" {
			//only second,minute,hour,day,week,month,quarter,year allowed
			groupByNeed = true

			selectColumnNameObj := ColumnNameStruct{}
			groupByColumnNameObj := selectColumnNameObj
			cft := ColumnFunctionType{}
			cft.BuildRollUpObj(qb.SQLLanguageLiterals.ByTimeBucket, "", val[0], qb.SQLLanguageLiterals.Language)

			selectColumnNameObj.BuildColumnNameStructObj(qb.SQLLanguageLiterals.TimeFieldName, "", "time_bucket", cft)
			selectColumnNameObjList = append(selectColumnNameObjList, selectColumnNameObj)

			groupByColumnNameObj.BuildColumnNameStructObj(qb.SQLLanguageLiterals.TimeFieldName, "", "", cft)
			groupByColumnNameObjList = append(groupByColumnNameObjList, groupByColumnNameObj)
		} else if key == "column" {
			selectColumnList := s.Split(val[0], ",")
			var colName string
			var colFunc string
			var colAlias string
			for _, c := range selectColumnList {

				if s.Index(c, ".") != -1 {
					if !groupByNeed {
						groupByNeed = true
					}
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

				cft.BuildColumnFunctionTypeObj(qb.returnFunctionName(colFunc), "")
				columnNameObj.BuildColumnNameStructObj(colName, "", colAlias, cft)
				selectColumnNameObjList = append(selectColumnNameObjList, columnNameObj)
				log.Println(groupByNeed)
				if colFunc == "" {
					log.Println("column to group by :", colName)
					columnNameObj.BuildColumnNameStructObj(colName, "", "", cft)
					groupByColumnNameObjList = append(groupByColumnNameObjList, columnNameObj)
				}
			}
			//check if group by needed, if not true , empty the already filled list
			if !groupByNeed {
				groupByColumnNameObjList = []ColumnNameStruct{}
			} else {
				qb.BuildGroupBy(groupByColumnNameObjList)
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
				100: {fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.AndKeyword) + opObjEndTime.FinalOperatorPhrase},
			}
		} else if key == "dataSource" {
			qb.SQLQuery.SQLTableName = val[0]

		}
	}

	qb.BuildFrom()

	delete(queryParametersURLValues, "startTime")
	delete(queryParametersURLValues, "limit")
	delete(queryParametersURLValues, "dataSource")
	delete(queryParametersURLValues, "by")

	groupNumForJSON := 0

	for key, val := range queryParametersURLValues {
		if key != "column" {
			for _, singleVal := range val {
				if string(singleVal[0]) == "{" && string(singleVal[len(singleVal)-1]) == "}" {
					//process json
					if qb.columnIsString(key) {
						var typeStruct StringJSON
						_ = json.Unmarshal([]byte(singleVal), &typeStruct)
						qb.SQLQuery.OperatorPhrase[groupNumForJSON] = []string{fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.AndKeyword)}
						qb.processStringJSONInput(key, typeStruct, groupNumForJSON)
						qb.SQLQuery.OperatorPhrase[groupNumForJSON] = append(qb.SQLQuery.OperatorPhrase[groupNumForJSON], ")")
					} else if qb.columnIsInt(key) {
						var typeStruct IntJSON
						_ = json.Unmarshal([]byte(singleVal), &typeStruct)
						qb.SQLQuery.OperatorPhrase[groupNumForJSON] = []string{fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.AndKeyword)}
						qb.processIntJSONInput(key, typeStruct, groupNumForJSON)
						qb.SQLQuery.OperatorPhrase[groupNumForJSON] = append(qb.SQLQuery.OperatorPhrase[groupNumForJSON], ")")
					}
				} else {
					//process array
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
						qb.processIntArrayInput(key, intArrayInput, groupNumForJSON)
					} else if qb.columnIsString(key) {
						qb.processStringArrayInput(key, stringArray, groupNumForJSON)
					}
				}
				groupNumForJSON++
			}
		}
	}

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

func (qb *Obj) returnFunctionName(functionName string) string {
	switch functionName {
	case "avg":
		return qb.SQLLanguageLiterals.Avg
	case "sum":
		return qb.SQLLanguageLiterals.Sum
	case "count":
		return qb.SQLLanguageLiterals.Count
	case "min":
		return qb.SQLLanguageLiterals.Min
	case "max":
		return qb.SQLLanguageLiterals.Max
	}
	return ""
}
