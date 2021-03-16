package querybuilder

/*
This package contains literals to be used by SQL language to construct the Select queries for LANTERN Api. There are no multiple joins with tables.
Single table query capability, with group by, having and order by capabilities.
*/

//SQLLanguageLiterals is a construct of what the literal struct  should look like
type SQLLanguageLiterals struct {
	Language         string // DRUID or POSTGRESQL
	TimestampLiteral string // TO_TIMESTAMP('str', 'format') for postgres and TIMESTAMP 'str' for druid
	IsNull           string // IS NULL for both
	IsNotNull        string // IS NOT NULL for both
	SelectKeyword    string // SELECT for both
	WhereKeyword     string // WHERE for both
	FromKeyword      string // FROM for both
	Contains         string // LIKE for both
	DoesNotContain   string // NOT LIKE for both
	EndsWith         string // LIKE for both
	DoesNotEndWith   string // NOT LIKE for both
	StartsWith       string // LIKE for both
	DoesNotStartWith string // NOT LIKE for both
	InList           string // IN for both
	NotInList        string // NOT IN for both
	LimitKeyWord     string // LIMIT for both
	AndKeyword       string // AND for both
	OrKeyword        string // OR for both
	GroupByKeyword   string // GROUP BY for both
	HavingKeyword    string // HAVING for both
	OrderByKeyWord   string // ORDER BY for both
	AscKeyword       string // ASC for both
	DescKeyword      string // DESC for both
	Alias            string // AS for both
	Avg              string // AVG() for both
	Min              string // MIN()
	Max              string // Max()
	Sum              string // SUM()
	Round            string // ROUND()
	Count            string // COUNT()
	Gt               string // >
	Lt               string // <
	Gte              string // >=
	Lte              string // <=
	Between          string // BETWEEN
	TimeFieldName    string // __time for druid, created_date for pg //need to ask Ryan/Anjaneya/Mike
	NumberType       []string
	StringType       []string
	EqualToInt       string
	NotEqualToInt    string
	EqualToString    string
	NotEqualToString string
	ByTimeBucket     string
	TimeBucketAlias  string
	TimeMaxEpoch     string
	TimeMinEpoch     string
}

//DruidSQLLanguageLiterals has all keywords understood in druid sql
var DruidSQLLanguageLiterals = SQLLanguageLiterals{
	Language:         "DRUIDSQL",
	SelectKeyword:    "SELECT",
	WhereKeyword:     "WHERE",
	FromKeyword:      "FROM",
	Contains:         "%s LIKE '%%%s%%'",
	DoesNotContain:   "%s NOT LIKE '%%%s%%'",
	EndsWith:         "%s LIKE '%%%s'",
	DoesNotEndWith:   "%s NOT LIKE '%%%s'",
	StartsWith:       "%s LIKE '%s%%'",
	DoesNotStartWith: "%s NOT LIKE '%s%%'",
	InList:           "%s IN (%v)",
	NotInList:        "%s NOT IN (%v)",
	LimitKeyWord:     " LIMIT",
	AndKeyword:       " AND",
	OrKeyword:        " OR",
	GroupByKeyword:   " GROUP BY",
	HavingKeyword:    " HAVING",
	OrderByKeyWord:   " ORDER BY",
	AscKeyword:       " ASC",
	DescKeyword:      " DESC",
	Alias:            " AS %s",
	Avg:              " AVG(%s)",
	Min:              " MIN(%s)",
	Max:              " MAX(%s)",
	Sum:              " SUM(%s)",
	Round:            " ROUND(%s,2)",
	Count:            " COUNT(%s)",
	Gt:               "%s > %v",
	Lt:               "%s < %v",
	Gte:              "%s >= %v",
	Lte:              "%s <= %v",
	Between:          "%s BETWEEN %s AND %s",
	TimeFieldName:    "__time",
	TimestampLiteral: "TIMESTAMP '%s'",
	IsNull:           "IS NULL",
	IsNotNull:        "IS NOT NULL",
	NumberType:       []string{"BIGINT", "FLOAT", "DOUBLE"},
	StringType:       []string{"VARCHAR"},
	EqualToInt:       "%s = %v",
	NotEqualToInt:    "%s <> %v",
	EqualToString:    "%s = '%v'",
	NotEqualToString: "%s <> '%v'",
	ByTimeBucket:     "FLOOR(%s to %s)",
	TimeBucketAlias:  "time_bucket",
	TimeMaxEpoch:     " EXTRACT(epoch from MAX(%s))",
	TimeMinEpoch:     " EXTRACT(epoch from MIN(%s))",
}

//PGSQLLanguageLiterals has all keywords understood in postgresq pgsql
var PGSQLLanguageLiterals = SQLLanguageLiterals{
	Language:         "POSTGRESQL",
	SelectKeyword:    "SELECT",
	WhereKeyword:     "WHERE",
	FromKeyword:      "FROM",
	Contains:         "%s LIKE '%%%s%%'",
	DoesNotContain:   "%s NOT LIKE '%%%s%%'",
	EndsWith:         "%s LIKE '%%%s'",
	DoesNotEndWith:   "%s NOT LIKE '%%%s'",
	StartsWith:       "%s LIKE '%s%%'",
	DoesNotStartWith: "%s NOT LIKE '%s%%'",
	InList:           "%s IN (%v)",
	NotInList:        "%s NOT IN (%v)",
	LimitKeyWord:     " LIMIT",
	AndKeyword:       " AND",
	OrKeyword:        " OR",
	GroupByKeyword:   " GROUP BY",
	HavingKeyword:    " HAVING",
	OrderByKeyWord:   " ORDER BY",
	AscKeyword:       " ASC",
	DescKeyword:      " DESC",
	Alias:            " AS %s",
	Avg:              " AVG(%s)",
	Min:              " MIN(%s)",
	Max:              " MAX(%s)",
	Sum:              " SUM (%s)",
	Round:            " ROUND(%s,2)",
	Count:            " COUNT(%s)",
	Gt:               "%s > %v",
	Lt:               "%s < %v",
	Gte:              "%s >= %v",
	Lte:              "%s <= %v",
	Between:          "%s BETWEEN %s AND %s",
	TimeFieldName:    "CREATED_DATE",
	TimestampLiteral: "TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS')",
	IsNull:           "IS NULL",
	IsNotNull:        "IS NOT NULL",
	NumberType:       []string{"bigint", "integer"},
	StringType:       []string{"bytea", "character varying", "text"},
	EqualToInt:       "%s = %v",
	NotEqualToInt:    "%s <> %v",
	EqualToString:    "%s = '%v'",
	NotEqualToString: "%s <> '%v'",
	ByTimeBucket:     "DATE_TRUNC('%s',%s)",
	TimeBucketAlias:  "time_bucket",
	TimeMaxEpoch:     " EXTRACT(epoch from MAX(%s))::int",
	TimeMinEpoch:     " EXTRACT(epoch from MIN(%s))::int",
}
