package querybuilder

// GroupByStruct struct for group by clause
type GroupByStruct struct {
	GroupByKeyword     string //group by
	GroupByColumns     []ColumnNameStruct
	FinalGroupByPhrase string //mix of above
}
