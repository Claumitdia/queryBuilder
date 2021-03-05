package querybuilder

import "errors"

// SQLQueryObj is an object that will hold the query
type SQLQueryObj struct {
	SelectPhrase   SelectStruct
	FromPhrase     FromStruct
	WherePhrase    WhereStruct
	GroupByPhrase  GroupByStruct
	HavingPhrase   map[int][]string
	OrderByPhrase  OrderByStruct
	LimitPhrase    LimitStruct
	OperatorPhrase map[int][]string
	StartTime      string
	EndTime        string
	AndOrPhrase    string
	SQLColumnTypes map[string]string
	SQLTableName   string
}

//QueryBuilderFunc - joins all phrases of QueryBuilder.SQLQuery to form final query string
func (qb *Obj) QueryBuilderFunc() (string, error) {
	var finalQuery string
	if qb.SQLQuery.SelectPhrase.FinalSelectPhrase != "" {
		finalQuery = qb.SQLQuery.SelectPhrase.FinalSelectPhrase
		if qb.SQLQuery.FromPhrase.FinalFromPhrase != "" {
			finalQuery += qb.SQLQuery.FromPhrase.FinalFromPhrase
			if qb.SQLQuery.WherePhrase.FinalWherePhrase != "" {
				finalQuery += qb.SQLQuery.WherePhrase.FinalWherePhrase
				//for and and or
				for _, opPhrase := range qb.SQLQuery.OperatorPhrase {
					for _, o := range opPhrase {
						finalQuery += o
					}
				}
				if qb.SQLQuery.GroupByPhrase.FinalGroupByPhrase != "" {
					finalQuery += qb.SQLQuery.GroupByPhrase.FinalGroupByPhrase
					if len(qb.SQLQuery.HavingPhrase) != 0 {
						for _, opPhrase := range qb.SQLQuery.HavingPhrase {
							for _, o := range opPhrase {
								finalQuery += o
							}
						}
					}
				}
				if qb.SQLQuery.OrderByPhrase.FinalOrderByPhrase != "" {
					finalQuery += qb.SQLQuery.OrderByPhrase.FinalOrderByPhrase
				}
			}
			if qb.SQLQuery.LimitPhrase.FinalLimitPhrase != "" {
				finalQuery += qb.SQLQuery.LimitPhrase.FinalLimitPhrase
			}
			return finalQuery, nil
		}
		return "", errors.New("ErrorQueryBuilder: No from clause")
	}
	return "", errors.New("ErrorQueryBuilder: No Query")
}
