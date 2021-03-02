package querybuilder

import (
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
	fromTableNameString := vars["dataSource"]

	queryParametersURLValues := r.URL.Query()
	queryParametersURLValues["dataSource"] = []string{fromTableNameString}
	var druidQueryBuilder q.Obj
	druidQueryBuilder.SQLLanguageLiterals = q.DruidSQLLanguageLiterals
	druidQueryBuilder.SQLBuilderFromURL(queryParametersURLValues)
	druidQuery, _ := druidQueryBuilder.QueryBuilderFunc()
	log.Println(druidQuery)

	// druidServerURL:= "http://xx.xxx.xxx.xxx:8888/druid/v2/sql"

}

//HandlerFuncPg is a handler for postgres
func HandlerFuncPg(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	fromTableNameString := vars["dataSource"]

	queryParametersURLValues := r.URL.Query()
	queryParametersURLValues["dataSource"] = []string{fromTableNameString}
	var pgQueryBuilder q.Obj
	pgQueryBuilder.SQLLanguageLiterals = q.PGSQLLanguageLiterals
	pgQueryBuilder.SQLBuilderFromURL(queryParametersURLValues)
	pgQuery, _ := pgQueryBuilder.QueryBuilderFunc()
	log.Println(pgQuery)

	var db *sql.DB
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable"
	db, _ = sql.Open("postgres", connStr)

	err := db.Ping()
	if err != nil {
		panic(err)
	}

	defer db.Close()
	stmt, err := db.Prepare("select jsonb_agg( g.*) from ( " + pgQuery + " ) g")
	if err != nil {
		panic(err)
	}
	rows, err := stmt.Query()
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
