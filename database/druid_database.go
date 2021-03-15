package database

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

type DruidDbObj struct {
	DruidServerURL string
}

func (d *DruidDbObj) DbConnect() error {
	return nil
}

func (d *DruidDbObj) DbQueryRun(queryString string) ([]map[string]string, error) {
	sqlPostRequest := map[string]string{
		"query": queryString,
	}

	reqBodyJSON, err := json.Marshal(sqlPostRequest)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	resp, err := http.Post(d.DruidServerURL, "application/json", bytes.NewBuffer(reqBodyJSON))
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	var data []map[string]string
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	return data, nil
}
