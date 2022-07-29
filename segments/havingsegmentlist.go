package segments

import (
	"sqlplus"
	"strings"
)

type HavingSegmentList struct {
	lastValue      sqlplus.ISqlSegment
	flushLastValue bool
	sqlSegments    []sqlplus.ISqlSegment
	sqlplus.ISqlSegment
}

func (having *HavingSegmentList) GetSqlSegment() string {
	if having.IsEmpty() {
		return sqlplus.EMPTY
	}
	var str = new(strings.Builder)
	for i, sqlSegment := range having.sqlSegments {
		str.WriteString(sqlSegment.GetSqlSegment())
		if i < len(having.sqlSegments)-1 {
			str.WriteString(sqlplus.SPACE)
		}
	}
	return sqlplus.SPACE + sqlplus.HAVING + sqlplus.SPACE + str.String()
}

func (having *HavingSegmentList) IsEmpty() bool {
	return len(having.sqlSegments) <= 0
}

func (having *HavingSegmentList) Add(sqlSegmentParams ...sqlplus.ISqlSegment) {
	firstSegment := sqlSegmentParams[0]
	goon, sqlSegmentReturn := having.TransformList(sqlSegmentParams, firstSegment)
	sqlSegmentParams = sqlSegmentReturn
	if goon {
		if having.flushLastValue {
			having.flushLastValueByList(sqlSegmentParams)
		}
		having.sqlSegments = append(having.sqlSegments, sqlSegmentParams...)
	}
}

func (having *HavingSegmentList) TransformList(sqlSegmentParams []sqlplus.ISqlSegment, firstSegment sqlplus.ISqlSegment) (bool, []sqlplus.ISqlSegment) {
	if !having.IsEmpty() {
		having.sqlSegments = append(having.sqlSegments, &sqlplus.SqlSegment{Keyword: sqlplus.AND})
	}
	return true, sqlSegmentParams[1:]
}

func (having *HavingSegmentList) RemoveAndFlushLast() {
	having.sqlSegments = append(having.sqlSegments[:len(having.sqlSegments)-1])
	having.flushLastValueByList(having.sqlSegments)
}

func (having *HavingSegmentList) flushLastValueByList(sqlSegmentParams []sqlplus.ISqlSegment) {
	having.lastValue = sqlSegmentParams[len(sqlSegmentParams)-1]
}
