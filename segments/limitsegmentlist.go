package segments

import (
	"sqlplus"
	"strings"
)

type LimitSegmentList struct {
	lastValue      sqlplus.ISqlSegment
	flushLastValue bool
	sqlSegments    []sqlplus.ISqlSegment
	sqlplus.ISqlSegment
}

func (limit *LimitSegmentList) GetSqlSegment() string {
	if limit.IsEmpty() {
		return sqlplus.EMPTY
	}
	var str = new(strings.Builder)
	for i, sqlSegment := range limit.sqlSegments {
		str.WriteString(sqlSegment.GetSqlSegment())
		if i < len(limit.sqlSegments)-1 {
			str.WriteString(sqlplus.COMMA)
		}
	}
	return sqlplus.SPACE + sqlplus.LIMIT + sqlplus.SPACE + str.String()
}

func (limit *LimitSegmentList) IsEmpty() bool {
	return len(limit.sqlSegments) <= 0
}

func (limit *LimitSegmentList) Add(sqlSegmentParams ...sqlplus.ISqlSegment) {
	firstSegment := sqlSegmentParams[0]
	goon, sqlSegmentReturn := limit.TransformList(sqlSegmentParams, firstSegment)
	sqlSegmentParams = sqlSegmentReturn
	if goon {
		if limit.flushLastValue {
			limit.flushLastValueByList(sqlSegmentParams)
		}
		limit.sqlSegments = append(limit.sqlSegments, sqlSegmentParams...)
	}
}

func (limit *LimitSegmentList) TransformList(sqlSegmentParams []sqlplus.ISqlSegment, firstSegment sqlplus.ISqlSegment) (bool, []sqlplus.ISqlSegment) {
	return true, sqlSegmentParams[1:]
}

func (limit *LimitSegmentList) RemoveAndFlushLast() {
	limit.sqlSegments = append(limit.sqlSegments[:len(limit.sqlSegments)-1])
	limit.flushLastValueByList(limit.sqlSegments)
}

func (limit *LimitSegmentList) flushLastValueByList(sqlSegmentParams []sqlplus.ISqlSegment) {
	limit.lastValue = sqlSegmentParams[len(sqlSegmentParams)-1]
}
