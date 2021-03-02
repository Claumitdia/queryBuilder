package querybuilder

// OrderByStruct is a struct for order by clause
type OrderByStruct struct {
	OrderByKeyword     string //order by
	OrderByColumn      ColumnNameStruct
	OrderByOrder       string //asc or desc
	FinalOrderByPhrase string //mix of above
}
