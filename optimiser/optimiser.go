package optimiser

import (
	"log"
	builder "queryBuilder/builder"
	db "queryBuilder/database"
	"sync"
	"time"
)

//Optimiser is an interface - will be implemented by optimisers for druid/pg/loginsight etc
// Anything implementing Optimiser will implement these functions,
type Optimiser interface {
	GetTimeFrameBucket(sqlObj builder.SQLQueryObj, db db.DbObj) (time.Duration, error)
	QueryTransformer(sqlObj builder.SQLQueryObj, timeBucket int) (map[int]string, error)
	ProcessQueriesAndMerge(queryMap map[int]string, db db.DbObj) ([]map[string]interface{}, error)
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
func (o *Obj) GetTimeFrameBucket(sqlObj builder.Obj, dbObj db.DbObj) (int64, error) {
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
	tempQueryBucket.SQLQuery.SQLTableName = "(" + finalQuery + ") f"
	tempQueryBucket.BuildFrom()
	finalQuery, finalQueryErr = tempQueryBucket.QueryBuilderFunc()
	log.Println(">>>>>>>>>>>>>:", finalQuery)
	if finalQueryErr != nil {
		log.Println("Error in building size query :", finalQueryErr)
		panic(finalQueryErr)
	}

	maxMinTimes, maxMinTimesErr := dbObj.DbQueryRun(finalQuery)
	if maxMinTimesErr != nil {
		panic(maxMinTimesErr)
	}

	log.Println("\n\n>>>>>>>>>>>:", maxMinTimes)
	//subtracting epoch
	maxTime, ok := maxMinTimes[0]["maxTime"].(float64)
	if !ok {
		log.Fatalln("max time assertion: ", ok)
		panic(ok)
	}
	minTime, ok := maxMinTimes[0]["minTime"].(float64)
	if !ok {
		log.Fatalln("min time assertion: ", ok)
		panic(ok)
	}

	maxTimeFinal := int64(maxTime)
	minTimeFinal := int64(minTime)
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
			// log.Printf("clause : %v %v", tempQueryBucket.SQLQuery.OperatorPhrase[0], tempQueryBucket.SQLQuery.WherePhrase.FinalWherePhrase)
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
			// log.Printf("helo :>> %v %v", time.Unix(stInternal, 0).Local().UTC().String()[:19], time.Unix(etInternal, 0).Local().UTC().String()[:19])
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

func (o *Obj) ProcessQueriesAndMerge(queryMap map[int]string, db db.DbObj) ([]map[string]interface{}, error) {
	log.Println("inside ProcessQueriesAndMerge ")
	queriesLength := len(queryMap)
	result := make(chan map[int][]map[string]interface{})
	mergedResult := make(chan map[int][]map[string]interface{})

	// // a channel to tell it to stop
	// stopchan := make(chan struct{})
	// // a channel to signal that it's stopped
	// stoppedchan := make(chan struct{})

	var wg sync.WaitGroup
	// waitGroup (no of druid goroutines + mergeData goroutine)
	wg.Add(queriesLength + 1)
	for key, query := range queryMap {
		// call goroutine
		go getData(key, query, result, &wg, db)
	}

	// goroutine to merge data parallelly
	go mergeData(result, &wg, queriesLength, mergedResult)

	fResult := <-mergedResult
	// log.Println("fResult ", fResult)
	var finalData []map[string]interface{}
	for i := 0; i < len(fResult); i++ {
		finalData = append(finalData, fResult[i]...)
	}
	close(mergedResult) // close channel
	return finalData, nil
}

func getData(key int, query string, result chan map[int][]map[string]interface{}, wg *sync.WaitGroup, db db.DbObj) {
	log.Println("goroutine started ... ", key)
	tempResult := make(map[int][]map[string]interface{})
	// open database connection
	db.DbConnect()
	// Run query
	data, err := db.DbQueryRun(query)
	if err != nil {
		panic(err)
	}
	tempResult[key] = data
	// close database connection
	db.DbClose()
	log.Println("go routine ended", key)
	wg.Done()
	result <- tempResult // pushing to result channel
}

func mergeData(result chan map[int][]map[string]interface{}, wg *sync.WaitGroup, queriesLength int, finalResult2 chan map[int][]map[string]interface{}) {
	log.Println("Inside merge data ")
	tempData := make(map[int][]map[string]interface{})
	for i := 0; i < queriesLength; i++ {
		// assigning data from result  channel to temp variable
		temp := <-result
		for k, v := range temp {
			tempData[k] = v // pushing all the results
		}
	}
	log.Println("Merge completed")
	wg.Done()
	finalResult2 <- tempData // pushing all the results to channel
	close(result)            // close channel
}
