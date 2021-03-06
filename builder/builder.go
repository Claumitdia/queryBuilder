package querybuilder

import (
	"encoding/json"
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

	var selectColumnNameObjList []ColumnNameStruct
	var groupByColumnNameObjList []ColumnNameStruct
	havingColumnNameObjList := map[string]string{}

	groupByNeed := false

	//process 'by'
	if len(queryParametersURLValues["by"]) != 0 {
		groupByNeed = true
		selectColumnNameObjList, groupByColumnNameObjList = qb.urlProcessBy(queryParametersURLValues["by"][0], selectColumnNameObjList, groupByColumnNameObjList)
	}
	log.Println("select stuff: ", selectColumnNameObjList)
	log.Println("group by stuff: ", groupByColumnNameObjList)
	//process 'column'
	if len(queryParametersURLValues["column"]) != 0 {
		selectColumnNameObjList, groupByColumnNameObjList, havingColumnNameObjList = qb.urlProcessColumn(queryParametersURLValues["column"][0], selectColumnNameObjList, groupByColumnNameObjList, havingColumnNameObjList, groupByNeed)
	}

	log.Println("group by stuff: ", groupByColumnNameObjList)
	qb.SQLQuery.SQLTableName = queryParametersURLValues["dataSource"][0]
	qb.BuildFrom()
	qb.BuildSelect(selectColumnNameObjList)
	qb.BuildGroupBy(groupByColumnNameObjList)
	//process 'limit'
	if len(queryParametersURLValues["limit"]) != 0 {
		qb.SQLQuery.LimitPhrase.LimitValue = qb.urlProcessLimit(queryParametersURLValues["limit"][0])
		qb.BuildLimit()
	}

	if len(queryParametersURLValues["endTime"]) == 0 {
		qb.calculateStartEndTime(queryParametersURLValues["startTime"][0], time.Now().Local().Format("2006-01-02 15:04:05"))
	} else {
		qb.calculateStartEndTime(queryParametersURLValues["startTime"][0], queryParametersURLValues["endTime"][0])
	}
	qb.SQLQuery.OperatorPhrase = map[int][]string{}
	qb.UrlProcessStartTime()
	//since already considered we can delete, will be present in sql query object of qb
	delete(queryParametersURLValues, "endTime")
	delete(queryParametersURLValues, "startTime")
	delete(queryParametersURLValues, "limit")
	delete(queryParametersURLValues, "dataSource")
	delete(queryParametersURLValues, "by")
	delete(queryParametersURLValues, "column")
	groupNumForJSON := 1
	countOfQueryParameters := 0
	countOfHavingQueryParameters := 0
	var having bool
	var newKey string
	qb.SQLQuery.HavingPhrase = map[int][]string{}
	//Last bit of processing

	for key, val := range queryParametersURLValues {
		//Adding 'HAVING ' for first condition in having clause
		//Adding 'AND ' for the first condition in normal condition clause
		if len(havingColumnNameObjList) != 0 {
			if _, ok := havingColumnNameObjList[key]; ok {
				having = true
				countOfHavingQueryParameters++
				if countOfHavingQueryParameters == 1 {
					qb.SQLQuery.HavingPhrase[groupNumForJSON] = append(qb.SQLQuery.HavingPhrase[groupNumForJSON], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.HavingKeyword))
				} else if countOfHavingQueryParameters > 1 {
					qb.SQLQuery.HavingPhrase[groupNumForJSON] = append(qb.SQLQuery.HavingPhrase[groupNumForJSON], fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.AndKeyword))
				}
			} else {
				having = false
				countOfQueryParameters++
			}
		} else {
			having = false
			countOfQueryParameters++
		}

		for _, singleVal := range val {
			if having {
				qb.SQLQuery.HavingPhrase[groupNumForJSON] = append(qb.SQLQuery.HavingPhrase[groupNumForJSON], "(")
			} else {
				qb.SQLQuery.OperatorPhrase[groupNumForJSON] = append(qb.SQLQuery.OperatorPhrase[groupNumForJSON], fmt.Sprintf(" %s (", qb.SQLLanguageLiterals.AndKeyword))
			}
			if string(singleVal[0]) == "{" && string(singleVal[len(singleVal)-1]) == "}" {
				//process json
				if qb.columnIsString(key) {
					// log.Println("is a string ")
					var typeStruct StringJSON
					_ = json.Unmarshal([]byte(singleVal), &typeStruct)
					if having {
						newKey = havingColumnNameObjList[key]
					} else {
						newKey = key
					}
					qb.processStringJSONInput(newKey, typeStruct, groupNumForJSON, having)
				} else if qb.columnIsInt(key) {
					// log.Println("is a int ")
					var typeStruct IntJSON
					_ = json.Unmarshal([]byte(singleVal), &typeStruct)
					if having {
						newKey = havingColumnNameObjList[key]
					} else {
						newKey = key
					}
					qb.processIntJSONInput(newKey, typeStruct, groupNumForJSON, having)
				}
			} else {
				// log.Println("is a int else  ")
				//process array
				stringArray := s.Split(singleVal, ",")
				// log.Println(key)
				// log.Println(qb.columnIsInt(key))
				// log.Println(qb.SQLLanguageLiterals.Language)
				if qb.columnIsInt(key) {
					// log.Println("is a int ")
					var intArrayInput []float64
					for _, vali := range stringArray {
						j, jerr := strconv.ParseFloat(vali, 64)
						// log.Println(j)
						if jerr != nil {
							// log.Println("intArrayInputErr :", jerr)
							log.Println(jerr)
						}
						intArrayInput = append(intArrayInput, j)
					}
					if having {
						newKey = havingColumnNameObjList[key]
					} else {
						newKey = key
					}
					// log.Println("intArrayInput:", intArrayInput)
					qb.processIntArrayInput(newKey, intArrayInput, groupNumForJSON, having)
				} else if qb.columnIsString(key) {
					// log.Println("is a string ")
					if having {
						newKey = havingColumnNameObjList[key]
					} else {
						newKey = key
					}
					qb.processStringArrayInput(newKey, stringArray, groupNumForJSON, having)
				}
			}
			if having {
				qb.SQLQuery.HavingPhrase[groupNumForJSON] = append(qb.SQLQuery.HavingPhrase[groupNumForJSON], ") ")
			} else {
				qb.SQLQuery.OperatorPhrase[groupNumForJSON] = append(qb.SQLQuery.OperatorPhrase[groupNumForJSON], ") ")
			}
			groupNumForJSON++
		}
	}

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

func (qb *Obj) urlProcessBy(val string, selectColumnNameObjList []ColumnNameStruct, groupByColumnNameObjList []ColumnNameStruct) ([]ColumnNameStruct, []ColumnNameStruct) {
	//only second,minute,hour,day,week,month,quarter,year allowed
	selectColumnNameObj := ColumnNameStruct{}
	groupByColumnNameObj := selectColumnNameObj
	cft := ColumnFunctionType{}
	cft.BuildRollUpObj(qb.SQLLanguageLiterals.ByTimeBucket, "", val, qb.SQLLanguageLiterals.Language)
	selectColumnNameObj.BuildColumnNameStructObj(qb.SQLLanguageLiterals.TimeFieldName, "", qb.SQLLanguageLiterals.TimeBucketAlias, cft)
	selectColumnNameObjList = append(selectColumnNameObjList, selectColumnNameObj)
	groupByColumnNameObj.BuildColumnNameStructObj(qb.SQLLanguageLiterals.TimeFieldName, "", "", cft)
	groupByColumnNameObjList = append(groupByColumnNameObjList, groupByColumnNameObj)
	return selectColumnNameObjList, groupByColumnNameObjList
}

func (qb *Obj) urlProcessColumn(val string, selectColumnNameObjList []ColumnNameStruct, groupByColumnNameObjList []ColumnNameStruct, havingColumnNameObjList map[string]string, groupByNeed bool) ([]ColumnNameStruct, []ColumnNameStruct, map[string]string) {
	selectColumnList := s.Split(val, ",")
	var colName string
	var colFunc string
	var colAlias string
	for _, c := range selectColumnList {
		if s.Index(c, ".") != -1 {
			if !groupByNeed && len(selectColumnList) > 1 {
				groupByNeed = true
			}
			colName = c[:s.Index(c, ".")]
			colFunc = c[(s.Index(c, ".") + 1):]
			colAlias = c
		} else {
			if c == "all" {
				colName = "*"
			} else {
				colName = c
			}
			colFunc = ""
			colAlias = ""
		}
		columnNameObj := ColumnNameStruct{}
		cft := ColumnFunctionType{}

		cft.BuildColumnFunctionTypeObj(qb.returnFunctionName(colFunc), "")
		columnNameObj.BuildColumnNameStructObj(colName, "", colAlias, cft)
		selectColumnNameObjList = append(selectColumnNameObjList, columnNameObj)
		if colFunc != "" {
			columnNameObj.BuildColumnNameStructObj(colName, "", "", cft)
			havingColumnNameObjList[colName] = columnNameObj.FinalColumnNamePhrase
		}
		if colFunc == "" {
			columnNameObj.BuildColumnNameStructObj(colName, "", "", cft)
			groupByColumnNameObjList = append(groupByColumnNameObjList, columnNameObj)
		}
	}
	if (groupByNeed == true && len(selectColumnList) == 0) || !groupByNeed {
		groupByColumnNameObjList = nil
	}
	return selectColumnNameObjList, groupByColumnNameObjList, havingColumnNameObjList
}

func (qb *Obj) urlProcessLimit(val string) int {
	limitVal, limitValErr := strconv.Atoi(val)
	if limitValErr != nil {
		panic(limitValErr)
	}
	return limitVal
}

func (qb *Obj) UrlProcessStartTime() {
	var opObjStartime OperatorStruct
	var opObjEndTime OperatorStruct
	columnNameObj := ColumnNameStruct{}
	cft := ColumnFunctionType{}
	cft.BuildColumnFunctionTypeObj("", "")
	columnNameObj.BuildColumnNameStructObj(qb.SQLLanguageLiterals.TimeFieldName, "timestamp", "", cft)
	opObjStartime.BuildOperatorString(columnNameObj, qb.SQLQuery.StartTime, qb.SQLLanguageLiterals.Gte, qb.SQLLanguageLiterals.Language)
	qb.BuildWhere(&opObjStartime, qb.SQLLanguageLiterals.WhereKeyword)
	opObjEndTime.BuildOperatorString(columnNameObj, qb.SQLQuery.EndTime, qb.SQLLanguageLiterals.Lte, qb.SQLLanguageLiterals.Language)
	qb.SQLQuery.OperatorPhrase[0] = []string{fmt.Sprintf(" %s ", qb.SQLLanguageLiterals.AndKeyword) + opObjEndTime.FinalOperatorPhrase}
}
