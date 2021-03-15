package database

type DbObj interface {
	DbConnect() error
	DbQueryRun(string) ([]map[string]string, error)
	DbClose() error
}
