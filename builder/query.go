package querybuilder

// SQLQueryObj is an object that will hold the query
type SQLQueryObj struct {
	SelectPhrase   SelectStruct
	FromPhrase     FromStruct
	WherePhrase    WhereStruct
	GroupByPhrase  GroupByStruct
	HavingPhrase   HavingStruct
	OrderByPhrase  OrderByStruct
	LimitPhrase    LimitStruct
	OperatorPhrase map[int][]string
	StartTime      string
	EndTime        string
	AndOrPhrase    string
	SQLColumnTypes map[string]string
	SQLTableName   string
}
