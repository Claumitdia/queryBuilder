package querybuilder

import (
	"strconv"
	s "strings"
	"time"
)

//calculateStartEndTime - calculates start and endtime and stores in QueryBuilder.SQLQuery.startTime and QueryBuilder.SQLQuery.endTime
func (qb *Obj) calculateStartEndTime(startTime string, endTime string) {
	var startTimeStr string
	var endTimeStr string
	var endTimeT time.Time
	var startTimeT time.Time
	endTimeT, endTimeTErr := time.Parse("2006-01-02 15:04:05", endTime)
	if endTimeTErr != nil {
		endTimeT, _ = time.Parse("2006-01-02 15:04:05", endTime+" 00:00:00")
	}
	if s.Contains(startTime, "-ago") {
		userDuration := startTime[:s.Index(startTime, "-ago")]
		userTimeSpanChar := string(userDuration[len(userDuration)-1])
		userTimeSpanInt, _ := strconv.Atoi(userDuration[:len(userDuration)-1])
		switch string(userTimeSpanChar) {
		case "s":
			startTimeT = endTimeT.Add(time.Second * time.Duration((-1 * userTimeSpanInt)))
		case "m":
			startTimeT = endTimeT.Add(time.Minute * time.Duration((-1 * userTimeSpanInt)))
		case "h":
			startTimeT = endTimeT.Add(time.Hour * time.Duration((-1 * userTimeSpanInt)))
		case "D":
			startTimeT = endTimeT.AddDate(0, 0, -userTimeSpanInt)
		case "M":
			startTimeT = endTimeT.AddDate(0, -userTimeSpanInt, 0)
		case "Y":
			startTimeT = endTimeT.AddDate(-userTimeSpanInt, 0, 0)
		}
	} else {
		startTimeT, endTimeTErr = time.Parse("2006-01-02 15:04:05", startTime)
		if endTimeTErr != nil {
			startTimeT, _ = time.Parse("2006-01-02 15:04:05", startTime+" 00:00:00")
		}
	}
	startTimeStr = startTimeT.String()
	endTimeStr = endTimeT.String()
	qb.SQLQuery.StartTime = startTimeStr[:19]
	qb.SQLQuery.EndTime = endTimeStr[:19]
}
