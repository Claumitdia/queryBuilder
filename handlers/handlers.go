package querybuilder

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	q "queryBuilder/builder"

	sql "database/sql"

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

	columnDatatyepeQuery := "SELECT distinct COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid' and TABLE_NAME ='" + queryParametersURLValues["dataSource"][0] + "'"
	// log.Println("druid column data type query : ", columnDatatyepeQuery)
	//get columns and their datatypes
	sqlPostRequest := map[string]string{
		"query": columnDatatyepeQuery,
	}

	reqBodyJSON, err := json.Marshal(sqlPostRequest)
	if err != nil {
		log.Println("panic here in reqbodyJson")
		panic(err)
	}
	resp, err := http.Post(druidServerURL, "application/json", bytes.NewBuffer(reqBodyJSON))
	if err != nil {
		log.Fatalln(err)
	}

	var data []map[string]string
	body, err1 := ioutil.ReadAll(resp.Body)
	err1 = json.Unmarshal(body, &data)
	if err1 != nil {
		panic("Decode error in handler for druid column datatype")
	}
	// log.Println("ColumnDataType map druid: ", data)
	finalColumnDtMap := map[string]string{}
	for _, columnDt := range data {
		finalColumnDtMap[columnDt["COLUMN_NAME"]] = columnDt["DATA_TYPE"]
	}

	// log.Println("ColumnDataType map druid: ", finalColumnDtMap)
	var druidQueryBuilder q.Obj
	druidQueryBuilder.SQLQuery.SQLColumnTypes = finalColumnDtMap
	druidQueryBuilder.SQLLanguageLiterals = q.DruidSQLLanguageLiterals
	druidQueryBuilder.SQLBuilderFromURL(queryParametersURLValues)
	druidQuery, _ := druidQueryBuilder.QueryBuilderFunc()
	log.Println(druidQuery)

	sqlPostRequest = map[string]string{
		"query": druidQuery,
	}
	reqBodyJSON, err = json.Marshal(sqlPostRequest)
	if err != nil {
		panic(err)
	}
	resp, err = http.Post(druidServerURL, "application/json", bytes.NewBuffer(reqBodyJSON))
	if err != nil {
		log.Fatalln(err)
	}
	body, err = ioutil.ReadAll(resp.Body)
	w.Header().Set("Content-type", "json")
	w.Write(body)
}

//HandlerFuncPg is a handler for postgres
func HandlerFuncPg(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	// fromTableNameString := vars["dataSource"]

	queryParametersURLValues := r.URL.Query()
	queryParametersURLValues["dataSource"] = []string{vars["dataSource"]}

	var db *sql.DB
	connStr := "host=sqlnlmetadata.amer.dell.com port=5432 user=ryan_morris1 password=FoolishPassword dbname=druid sslmode=disable"
	db, _ = sql.Open("postgres", connStr)
	defer db.Close()
	err := db.Ping()
	if err != nil {
		panic(err)
	}

	stmt, err := db.Prepare("SELECT distinct column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'public' and Table_name='" + queryParametersURLValues["dataSource"][0] + "'")
	if err != nil {
		panic(err)
	}
	rows, err := stmt.Query()
	if err != nil {
		panic(err)
	}

	ColumnDataType := map[string]string{}
	var columnName string
	var dataType string
	for rows.Next() {
		rows.Scan(&columnName, &dataType)
		ColumnDataType[columnName] = dataType
	}
	log.Println("ColumnDataType map pg: ", ColumnDataType)

	var pgQueryBuilder q.Obj
	pgQueryBuilder.SQLLanguageLiterals = q.PGSQLLanguageLiterals
	pgQueryBuilder.SQLQuery.SQLColumnTypes = ColumnDataType
	pgQueryBuilder.SQLBuilderFromURL(queryParametersURLValues)
	pgQuery, _ := pgQueryBuilder.QueryBuilderFunc()
	log.Println(pgQuery)

	stmt, err = db.Prepare("select jsonb_agg( g.*) from ( " + pgQuery + " ) g")
	if err != nil {
		panic(err)
	}
	rows, err = stmt.Query()
	if err != nil {
		panic(err)
	}
	var results string
	for rows.Next() {
		rows.Scan(&results)
	}
	w.Header().Set("Content-type", "json")
	w.Write([]byte(results))
}
