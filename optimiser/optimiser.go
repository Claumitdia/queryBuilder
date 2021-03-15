package optimiser

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	builder "queryBuilder/builder"
	"time"
)

//Optimiser is an interface - will be implemented by optimisers for druid/pg/loginsight etc
// Anything implementing Optimiser will implement these functions,
type Optimiser interface {
	GetTimeFrameBucket(builder.SQLQueryObj, *DbObj) (time.Duration, error)
	QueryTransformer(builder.SQLQueryObj, timeBucket int) (map[int]string, error)
	ProcessQueriesAndMerge(map[int]string, *DbObj) (interface{}, error)
}

//Limits will hold values like
// maxCount - max number of records above which optimiser will be called
// optimalCount - max number of records in a bucket
// timeCheckDuration - difference between endTime and startTime, above which optimiser will be called
type Limits struct {
	OptimalCount      int
	MaxCount          int
	TimeCheckDuration time.Duration
}

//Obj is a structure that implements Optimiser interface
type Obj struct {
	LimitsObj Limits
	QueryObj  builder.SQLQueryObj
}

//GetTimeFrameBucket - function gets time frame bucket from query, in minutes/hours/seconds .TBD
//This functions assumes the query is not one with group by, having clauses. but can have aggregate functions.
func (o *Obj) GetTimeFrameBucket(sqlObj builder.Obj,dbObj DbObj) (int64, error) {
	tempQueryBucket := sqlObj
	tempQueryBucket.SQLQuery.StartTime = sqlObj.SQLQuery.StartTime
	tempQueryBucket.SQLQuery.EndTime = sqlObj.SQLQuery.EndTime
	tempQueryBucket.SQLQuery.LimitPhrase.LimitValue = o.LimitsObj.OptimalCount

	var timeFunc builder.ColumnFunctionType
	var timeCol builder.ColumnNameStruct

	timeFunc.BuildColumnFunctionTypeObj("", "")
	timeCol.BuildColumnNameStructObj(tempQueryBucket.SQLLanguageLiterals.TimeFieldName, "", "", timeFunc)
	tempQueryBucket.SQLQuery.SelectPhrase.ColumnNames = nil
	tempQueryBucket.SQLQuery.SelectPhrase.ColumnNames = append(tempQueryBucket.SQLQuery.SelectPhrase.ColumnNames, timeCol)
	tempQueryBucket.BuildLimit()
	tempQueryBucket.BuildSelect(tempQueryBucket.SQLQuery.SelectPhrase.ColumnNames)
	finalQuery, finalQueryErr := tempQueryBucket.QueryBuilderFunc()
	if finalQueryErr != nil {
		log.Fatalln("Error from GetTimeFrameBucket : ", finalQueryErr.Error())
	}

	tempQueryBucket = builder.Obj{}
	tempQueryBucket.SQLLanguageLiterals = sqlObj.SQLLanguageLiterals

	var epochMaxTime builder.ColumnNameStruct
	var epochMaxTimeF builder.ColumnFunctionType
	var epochMinTime builder.ColumnNameStruct
	var epochMinTimeF builder.ColumnFunctionType
	epochMaxTimeF.BuildColumnFunctionTypeObj(tempQueryBucket.SQLLanguageLiterals.TimeMaxEpoch, "")
	epochMaxTime.BuildColumnNameStructObj(tempQueryBucket.SQLLanguageLiterals.TimeFieldName, "", "maxTime", epochMaxTimeF)
	epochMinTimeF.BuildColumnFunctionTypeObj(tempQueryBucket.SQLLanguageLiterals.TimeMinEpoch, "")
	epochMinTime.BuildColumnNameStructObj(tempQueryBucket.SQLLanguageLiterals.TimeFieldName, "", "minTime", epochMinTimeF)

	tempQueryBucket.SQLQuery.SelectPhrase.ColumnNames = nil
	tempQueryBucket.SQLQuery.SelectPhrase.ColumnNames = append(tempQueryBucket.SQLQuery.SelectPhrase.ColumnNames, epochMaxTime, epochMinTime)
	tempQueryBucket.BuildSelect(tempQueryBucket.SQLQuery.SelectPhrase.ColumnNames)
	tempQueryBucket.SQLQuery.SQLTableName = "(" + finalQuery + ")"
	tempQueryBucket.BuildFrom()
	log.Println("from final phrase : ", tempQueryBucket.SQLQuery.FromPhrase.FinalFromPhrase)
	finalQuery, finalQueryErr = tempQueryBucket.QueryBuilderFunc()
	if finalQueryErr != nil {
		log.Println("Error in building size query :", finalQueryErr)
		panic(finalQueryErr)
	}
	sqlPostRequest := map[string]string{
		"query": finalQuery,
	}

	log.Println("Final Query for size from GetTimeFrameBucket :", finalQuery)
	reqBodyJSON, err := json.Marshal(sqlPostRequest)
	if err != nil {
		panic(err)
	}

	druidServerURL := "http://10.179.206.156:8888/druid/v2/sql"
	resp, err := http.Post(druidServerURL, "application/json", bytes.NewBuffer(reqBodyJSON))
	if err != nil {
		log.Fatalln(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	var maxMinTimes []map[string]int64
	maxMinTimesErr := json.Unmarshal(body, &maxMinTimes)
	if maxMinTimesErr != nil {
		panic(maxMinTimesErr)
	}

	log.Println("\n max min time:", maxMinTimes)

	//subtracting epoch
	maxTimeFinal := int64(maxMinTimes[0]["maxTime"])
	minTimeFinal := int64(maxMinTimes[0]["minTime"])

	epochInterval := maxTimeFinal - minTimeFinal

	log.Println("epochInterval :", epochInterval)
	return epochInterval, nil
}

//QueryTransformer creates the queries with the above obtained timeinterval in epoch, returns a map of id with query, size of map is number of buckets
func (o *Obj) QueryTransformer(sqlObj builder.Obj, timeBucket int64) (map[int]string, error) {
	concurrentQueryMap := map[int]string{}
	tempQueryBucket := sqlObj
	st, stErr := time.Parse("2006-01-02 15:04:05", tempQueryBucket.SQLQuery.StartTime)
	if stErr != nil {
		log.Println(stErr)
	}
	log.Println(st)
	et, etErr := time.Parse("2006-01-02 15:04:05", tempQueryBucket.SQLQuery.EndTime)
	if etErr != nil {
		log.Println(etErr)
	}
	stUnix := st.UTC().Unix()
	etUnix := et.UTC().Unix()
	stInternal := stUnix
	etInternal := stUnix
	queryID := 0
	for {
		if etInternal+timeBucket < etUnix {
			stInternal = etInternal
			etInternal = etInternal + timeBucket
			tempQueryBucket.SQLQuery.StartTime = time.Unix(stInternal, 0).Local().UTC().String()[:19]
			tempQueryBucket.SQLQuery.EndTime = time.Unix(etInternal, 0).Local().UTC().String()[:19]
			tempQueryBucket.UrlProcessStartTime()
			log.Printf("clause : %v %v", tempQueryBucket.SQLQuery.OperatorPhrase[0], tempQueryBucket.SQLQuery.WherePhrase.FinalWherePhrase)
			finalQuery, finalQueryErr := tempQueryBucket.QueryBuilderFunc()
			if finalQueryErr != nil {
				log.Println(finalQueryErr)
				panic(finalQueryErr)
			}
			concurrentQueryMap[queryID] = finalQuery
			queryID++
		} else {
			stInternal = etInternal
			etInternal = etUnix
			log.Printf("%v %v", time.Unix(stInternal, 0), time.Unix(etInternal, 0))
			tempQueryBucket.SQLQuery.StartTime = time.Unix(stInternal, 0).Local().UTC().String()[:19]
			tempQueryBucket.SQLQuery.EndTime = time.Unix(etInternal, 0).Local().UTC().String()[:19]
			tempQueryBucket.UrlProcessStartTime()
			finalQuery, finalQueryErr := tempQueryBucket.QueryBuilderFunc()
			if finalQueryErr != nil {
				log.Println(finalQueryErr)
				panic(finalQueryErr)
			}
			concurrentQueryMap[queryID] = finalQuery
			break
		}
	}
	return concurrentQueryMap, nil
}
