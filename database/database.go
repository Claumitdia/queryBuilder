package database

type DbObj interface {
	DbConnect() error
	DbQueryRun(string) ([]map[string]interface{}, error)
	DbClose() error
}
