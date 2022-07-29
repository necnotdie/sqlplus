package segments

import (
	"sqlplus"
	"strings"
)

type OrderBySegmentList struct {
	lastValue      sqlplus.ISqlSegment
	flushLastValue bool
	sqlSegments    []sqlplus.ISqlSegment
	sqlplus.ISqlSegment
}

func (orderBy *OrderBySegmentList) GetSqlSegment() string {
	if orderBy.IsEmpty() {
		return sqlplus.EMPTY
	}
	var str = new(strings.Builder)
	for i, sqlSegment := range orderBy.sqlSegments {
		str.WriteString(sqlSegment.GetSqlSegment())
		if i < len(orderBy.sqlSegments)-1 {
			str.WriteString(sqlplus.SPACE)
		}
	}
	return sqlplus.SPACE + sqlplus.ORDER_BY + sqlplus.SPACE + str.String()
}

func (orderBy *OrderBySegmentList) IsEmpty() bool {
	return len(orderBy.sqlSegments) <= 0
}

func (orderBy *OrderBySegmentList) Add(sqlSegmentParams ...sqlplus.ISqlSegment) {
	firstSegment := sqlSegmentParams[0]
	goon, sqlSegmentReturn := orderBy.TransformList(sqlSegmentParams, firstSegment)
	sqlSegmentParams = sqlSegmentReturn
	if goon {
		if orderBy.flushLastValue {
			orderBy.flushLastValueByList(sqlSegmentParams)
		}
		orderBy.sqlSegments = append(orderBy.sqlSegments, sqlSegmentParams...)
	}
}

func (orderBy *OrderBySegmentList) TransformList(sqlSegmentParams []sqlplus.ISqlSegment, firstSegment sqlplus.ISqlSegment) (bool, []sqlplus.ISqlSegment) {
	if !orderBy.IsEmpty() {
		orderBy.sqlSegments = append(orderBy.sqlSegments, &sqlplus.SqlSegment{Keyword: sqlplus.COMMA})
	}
	return true, sqlSegmentParams[1:]
}

func (orderBy *OrderBySegmentList) RemoveAndFlushLast() {
	orderBy.sqlSegments = append(orderBy.sqlSegments[:len(orderBy.sqlSegments)-1])
	orderBy.flushLastValueByList(orderBy.sqlSegments)
}

func (orderBy *OrderBySegmentList) flushLastValueByList(sqlSegmentParams []sqlplus.ISqlSegment) {
	orderBy.lastValue = sqlSegmentParams[len(sqlSegmentParams)-1]
}
