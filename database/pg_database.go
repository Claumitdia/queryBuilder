package database

import (
	sql "database/sql"
	"encoding/json"
	"log"

	_ "github.com/lib/pq"
)

type PgDbObj struct {
	ConnectionString string
	Db               *sql.DB
}

func (d *PgDbObj) DbConnect() error {
	dbConn, err := sql.Open("postgres", d.ConnectionString)
	d.Db = dbConn
	if err != nil {
		log.Fatalln(err)
		return err
	}
	err = d.Db.Ping()
	if err != nil {
		log.Fatalln(err)
		return err
	}
	return nil
}

func (d *PgDbObj) DbQueryRun(queryString string) ([]map[string]interface{}, error) {
	stmt, err := d.Db.Prepare("select jsonb_agg( g.*) from ( " + queryString + " ) g")
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	rows, err := stmt.Query()
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	var results string
	for rows.Next() {
		rows.Scan(&results)
	}

	var queryResults []map[string]interface{}
	err = json.Unmarshal([]byte(results), &queryResults)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	return queryResults, nil
}

func (d *PgDbObj) DbClose() error {
	err := d.Db.Close()
	if err != nil {
		log.Fatalln(err)
		return err
	}
	return nil
}
