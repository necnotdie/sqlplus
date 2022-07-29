package segments

import (
	"sqlplus"
	"strings"
)

type GroupBySegmentList struct {
	lastValue      sqlplus.ISqlSegment
	flushLastValue bool
	sqlSegments    []sqlplus.ISqlSegment
	sqlplus.ISqlSegment
}

func (groupBy *GroupBySegmentList) GetSqlSegment() string {
	if groupBy.IsEmpty() {
		return sqlplus.EMPTY
	}
	var str = new(strings.Builder)
	for i, sqlSegment := range groupBy.sqlSegments {
		str.WriteString(sqlSegment.GetSqlSegment())
		if i < len(groupBy.sqlSegments)-1 {
			str.WriteString(sqlplus.COMMA)
		}
	}
	return sqlplus.SPACE + sqlplus.GROUP_BY + sqlplus.SPACE + str.String()
}

func (groupBy *GroupBySegmentList) IsEmpty() bool {
	return len(groupBy.sqlSegments) <= 0
}

func (groupBy *GroupBySegmentList) Add(sqlSegmentParams ...sqlplus.ISqlSegment) {
	firstSegment := sqlSegmentParams[0]
	goon, sqlSegmentReturn := groupBy.TransformList(sqlSegmentParams, firstSegment)
	sqlSegmentParams = sqlSegmentReturn
	if goon {
		if groupBy.flushLastValue {
			groupBy.flushLastValueByList(sqlSegmentParams)
		}
		groupBy.sqlSegments = append(groupBy.sqlSegments, sqlSegmentParams...)
	}
}

func (groupBy *GroupBySegmentList) TransformList(sqlSegmentParams []sqlplus.ISqlSegment, firstSegment sqlplus.ISqlSegment) (bool, []sqlplus.ISqlSegment) {
	return true, sqlSegmentParams[1:]
}

func (groupBy *GroupBySegmentList) RemoveAndFlushLast() {
	groupBy.sqlSegments = append(groupBy.sqlSegments[:len(groupBy.sqlSegments)-1])
	groupBy.flushLastValueByList(groupBy.sqlSegments)
}

func (groupBy *GroupBySegmentList) flushLastValueByList(sqlSegmentParams []sqlplus.ISqlSegment) {
	groupBy.lastValue = sqlSegmentParams[len(sqlSegmentParams)-1]
}
