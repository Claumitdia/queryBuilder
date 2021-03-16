package querybuilder

import (
	"encoding/json"
	"log"
	"net/http"
	q "queryBuilder/builder"
	p "queryBuilder/optimiser"

	db "queryBuilder/database"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

//HandlerFuncDruid is a handler for druid
func HandlerFuncDruid(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	// fromTableNameString := vars["dataSource"]
	queryParametersURLValues := r.URL.Query()
	queryParametersURLValues["dataSource"] = []string{vars["dataSource"]}
	druidServerURL := "http://10.179.206.156:8888/druid/v2/sql"
	var druidDbObj db.DruidDbObj
	druidDbObj.DruidServerURL = druidServerURL
	columnDatatyepeQuery := "SELECT distinct COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid' and TABLE_NAME ='" + queryParametersURLValues["dataSource"][0] + "'"
	data, err := druidDbObj.DbQueryRun(columnDatatyepeQuery)
	if err != nil {
		panic(err)
	}
	finalColumnDtMap := map[string]string{}

	for _, columnDt := range data {
		cName, ok := columnDt["COLUMN_NAME"].(string)
		if !ok {
			log.Fatalln("column name :", ok)
			panic(ok)
		}
		cType, ok := columnDt["DATA_TYPE"].(string)
		if !ok {
			log.Fatalln("column type :", ok)
			panic(ok)
		}
		finalColumnDtMap[cName] = cType
	}

	var druidQueryBuilder q.Obj
	druidQueryBuilder.SQLQuery.SQLColumnTypes = finalColumnDtMap
	druidQueryBuilder.SQLLanguageLiterals = q.DruidSQLLanguageLiterals
	druidQueryBuilder.SQLBuilderFromURL(queryParametersURLValues)
	druidQuery, _ := druidQueryBuilder.QueryBuilderFunc()
	log.Println(druidQuery)

	results, err := druidDbObj.DbQueryRun(druidQuery)
	if err != nil {
		panic(err)
	}
	w.Header().Set("Content-type", "json")
	resultsBytes, err := json.Marshal(results)
	if err != nil {
		panic(err)
	}
	w.Write(resultsBytes)

	//testing optimiser
	var druidOptimiser p.Obj
	druidOptimiser.LimitsObj.OptimalCount = 140
	e, _ := druidOptimiser.GetTimeFrameBucket(druidQueryBuilder, &druidDbObj)
	mapQueries, mapQueriesErr := druidOptimiser.QueryTransformer(druidQueryBuilder, e)
	log.Println("\n\n\n\nLength of Map of Queries:", len(mapQueries))
	// for idx, val := range mapQueries {
	// 	log.Printf("%v : %v", idx, val)
	// }
	if mapQueriesErr != nil {
		panic(mapQueriesErr)
	}

}

//HandlerFuncPg is a handler for postgres
func HandlerFuncPg(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	queryParametersURLValues := r.URL.Query()
	queryParametersURLValues["dataSource"] = []string{vars["dataSource"]}

	var pgDbObj db.PgDbObj
	connStr := "host=sqlnlmetadata.amer.dell.com port=5432 user=ryan_morris1 password=FoolishPassword dbname=druid sslmode=disable"
	query := "SELECT distinct column_name as \"COLUMN_NAME\", data_type as \"DATA_TYPE\" FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'public' and Table_name='" + queryParametersURLValues["dataSource"][0] + "'"
	pgDbObj.ConnectionString = connStr
	conectionErr := pgDbObj.DbConnect()
	if conectionErr != nil {
		log.Fatalln(conectionErr)
		panic(conectionErr)
	}
	data, err := pgDbObj.DbQueryRun(query)
	if err != nil {
		log.Fatalln(err)
		panic(err)
	}
	log.Println("data map pg: ", data)
	var pgQueryBuilder q.Obj
	pgQueryBuilder.SQLLanguageLiterals = q.PGSQLLanguageLiterals
	finalColumnDtMap := map[string]string{}
	for _, columnDt := range data {
		cName, ok := columnDt["COLUMN_NAME"].(string)
		if !ok {
			log.Fatalln("column name :", ok)
			panic(ok)
		}
		cType, ok := columnDt["DATA_TYPE"].(string)
		if !ok {
			log.Fatalln("column type :", ok)
			panic(ok)
		}
		finalColumnDtMap[cName] = cType
	}
	pgQueryBuilder.SQLQuery.SQLColumnTypes = finalColumnDtMap
	pgQueryBuilder.SQLBuilderFromURL(queryParametersURLValues)

	pgQuery, err := pgQueryBuilder.QueryBuilderFunc()
	if err != nil {
		log.Fatalln(err)
		panic(err)
	}
	log.Println(pgQuery)
	results, err := pgDbObj.DbQueryRun(query)
	if err != nil {
		log.Fatalln(err)
		panic(err)
	}
	resultsString, err := json.Marshal(results)
	if err != nil {
		log.Fatalln(err)
		panic(err)
	}
	w.Header().Set("Content-type", "json")
	w.Write(resultsString)

	//testing optimiser
	var pgOptimiser p.Obj
	pgOptimiser.LimitsObj.OptimalCount = 150
	e, err := pgOptimiser.GetTimeFrameBucket(pgQueryBuilder, &pgDbObj)
	if err != nil {
		log.Fatalln(err)
		panic(err)
	}
	mapQueries, mapQueriesErr := pgOptimiser.QueryTransformer(pgQueryBuilder, e)
	log.Println("\n\n\n\nLength of Map of Queries:", len(mapQueries))
	// for idx, val := range mapQueries {
	// 	log.Printf("%v : %v", idx, val)
	// }
	if mapQueriesErr != nil {
		panic(mapQueriesErr)
	}
}
