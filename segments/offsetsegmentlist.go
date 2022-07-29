package segments

import (
	"sqlplus"
	"strings"
)

type OffsetSegmentList struct {
	lastValue      sqlplus.ISqlSegment
	flushLastValue bool
	sqlSegments    []sqlplus.ISqlSegment
	sqlplus.ISqlSegment
}

func (offset *OffsetSegmentList) GetSqlSegment() string {
	if offset.IsEmpty() {
		return sqlplus.EMPTY
	}
	var str = new(strings.Builder)
	for i, sqlSegment := range offset.sqlSegments {
		str.WriteString(sqlSegment.GetSqlSegment())
		if i < len(offset.sqlSegments)-1 {
			str.WriteString(sqlplus.COMMA)
		}
	}
	return sqlplus.SPACE + sqlplus.OFFSET + sqlplus.SPACE + str.String()
}

func (offset *OffsetSegmentList) IsEmpty() bool {
	return len(offset.sqlSegments) <= 0
}

func (offset *OffsetSegmentList) Add(sqlSegmentParams ...sqlplus.ISqlSegment) {
	firstSegment := sqlSegmentParams[0]
	goon, sqlSegmentReturn := offset.TransformList(sqlSegmentParams, firstSegment)
	sqlSegmentParams = sqlSegmentReturn
	if goon {
		if offset.flushLastValue {
			offset.flushLastValueByList(sqlSegmentParams)
		}
		offset.sqlSegments = append(offset.sqlSegments, sqlSegmentParams...)
	}
}

func (offset *OffsetSegmentList) TransformList(sqlSegmentParams []sqlplus.ISqlSegment, firstSegment sqlplus.ISqlSegment) (bool, []sqlplus.ISqlSegment) {
	return true, sqlSegmentParams[1:]
}

func (offset *OffsetSegmentList) RemoveAndFlushLast() {
	offset.sqlSegments = append(offset.sqlSegments[:len(offset.sqlSegments)-1])
	offset.flushLastValueByList(offset.sqlSegments)
}

func (offset *OffsetSegmentList) flushLastValueByList(sqlSegmentParams []sqlplus.ISqlSegment) {
	offset.lastValue = sqlSegmentParams[len(sqlSegmentParams)-1]
}
