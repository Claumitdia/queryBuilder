package database

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

type DruidException struct {
	// Error        string `json:"error"`
	ErrorMessage string `json:"errorMessage"`
	ErrorClass   string `json:"errorClass"`
}

func (d DruidException) Error() string {
	return d.ErrorMessage
}

type DruidDbObj struct {
	DruidServerURL string
}

func (d *DruidDbObj) DbConnect() error {
	return nil
}

func (d *DruidDbObj) DbQueryRun(queryString string) ([]map[string]interface{}, error) {
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

	var data []map[string]interface{}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Println("Unmarshal error : ", err)
		var druidException DruidException
		unmarshallErr := json.Unmarshal(body, &druidException)
		if unmarshallErr != nil {
			return nil, druidException
		}
		return nil, druidException
	}
	return data, nil
}

func (d *DruidDbObj) DbClose() error {
	return nil
}
