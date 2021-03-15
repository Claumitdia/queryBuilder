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
		finalColumnDtMap[columnDt["COLUMN_NAME"]] = columnDt["DATA_TYPE"]
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
	_, mapQueriesErr := druidOptimiser.QueryTransformer(druidQueryBuilder, e)
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
	query := "SELECT distinct column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'public' and Table_name='" + queryParametersURLValues["dataSource"][0] + "'"
	pgDbObj.ConnectionString = connStr
	conectionErr := pgDbObj.DbConnect()
	if conectionErr != nil {
		log.Fatalln(conectionErr)
		panic(conectionErr)
	}
	ColumnDataType, err := pgDbObj.DbQueryRun(query)
	if err != nil {
		log.Fatalln(err)
		panic(err)
	}
	log.Println("ColumnDataType map pg: ", ColumnDataType)
	var pgQueryBuilder q.Obj
	pgQueryBuilder.SQLLanguageLiterals = q.PGSQLLanguageLiterals
	pgQueryBuilder.SQLQuery.SQLColumnTypes = ColumnDataType[0]
	pgQueryBuilder.SQLBuilderFromURL(queryParametersURLValues)

	var pgOptimiser p.Obj
	pgOptimiser.LimitsObj.OptimalCount = 10000

	e, err := pgOptimiser.GetTimeFrameBucket(pgQueryBuilder, &pgDbObj)
	if err != nil {
		log.Fatalln(err)
		panic(err)
	}
	pgOptimiser.QueryTransformer(pgQueryBuilder, e)

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
}
