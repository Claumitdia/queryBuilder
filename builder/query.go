package querybuilder

// SQLQueryObj is an object that will hold the query
type SQLQueryObj struct {
	SelectPhrase  SelectStruct
	FromPhrase    FromStruct
	WherePhrase   WhereStruct
	AndPhrase     []AndStruct
	OrPhrase      []OrStruct
	GroupByPhrase GroupByStruct
	HavingPhrase  HavingStruct
	OrderByPhrase OrderByStruct
	LimitPhrase   LimitStruct
	StartTime     string
	EndTime       string
}
