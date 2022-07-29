package segments

import (
	"sqlplus"
	"strings"
)

type NormalSegmentList struct {
	lastValue      sqlplus.ISqlSegment
	flushLastValue bool
	sqlSegments    []sqlplus.ISqlSegment
	executeNot     bool
	sqlplus.ISqlSegment
}

func (normal *NormalSegmentList) GetSqlSegment() string {
	if normal.lastValue.GetSqlSegment() == sqlplus.AND || normal.lastValue.GetSqlSegment() == sqlplus.OR {
		normal.RemoveAndFlushLast()
	}
	var str = new(strings.Builder)
	for i, sqlSegment := range normal.sqlSegments {
		str.WriteString(sqlSegment.GetSqlSegment())
		if i < len(normal.sqlSegments)-1 {
			str.WriteString(sqlplus.SPACE)
		}
	}
	sqlStr := str.String()
	if sqlStr[0:] != sqlplus.LEFT_BRACKET || sqlStr[len(sqlStr)-1:] != sqlplus.RIGHT_BRACKET {
		sqlStr = sqlplus.LEFT_BRACKET + sqlStr + sqlplus.RIGHT_BRACKET
	}
	return sqlStr
}

func (normal *NormalSegmentList) IsEmpty() bool {
	return len(normal.sqlSegments) <= 0
}

func (normal *NormalSegmentList) Add(sqlSegmentParams ...sqlplus.ISqlSegment) {
	firstSegment := sqlSegmentParams[0]
	goon, sqlSegmentReturn := normal.TransformList(sqlSegmentParams, firstSegment)
	sqlSegmentParams = sqlSegmentReturn
	if goon {
		if normal.flushLastValue {
			normal.flushLastValueByList(sqlSegmentParams)
		}
		normal.sqlSegments = append(normal.sqlSegments, sqlSegmentParams...)
	}
}

func (normal *NormalSegmentList) TransformList(sqlSegmentParams []sqlplus.ISqlSegment, firstSegment sqlplus.ISqlSegment) (bool, []sqlplus.ISqlSegment) {
	if len(sqlSegmentParams) == 1 {
		if firstSegment.GetSqlSegment() != sqlplus.NOT {
			if normal.IsEmpty() {
				return false, sqlSegmentParams
			}
			matchLastAnd := normal.lastValue.GetSqlSegment() == sqlplus.AND
			matchLastOr := normal.lastValue.GetSqlSegment() == sqlplus.OR
			if matchLastAnd || matchLastOr {
				if matchLastAnd && firstSegment.GetSqlSegment() == sqlplus.AND {
					return false, sqlSegmentParams
				} else if matchLastOr && firstSegment.GetSqlSegment() == sqlplus.OR {
					return false, sqlSegmentParams
				} else {
					normal.RemoveAndFlushLast()
				}
			}
		} else {
			normal.executeNot = false
			return false, sqlSegmentParams
		}
	} else {
		if !normal.executeNot {
			if firstSegment.GetSqlSegment() == sqlplus.EXISTS {
				sqlSegmentParams = append([]sqlplus.ISqlSegment{&sqlplus.SqlSegment{Keyword: sqlplus.NOT}}, sqlSegmentParams...)
			} else {
				sqlSegmentParams = append(sqlSegmentParams[:1], append([]sqlplus.ISqlSegment{&sqlplus.SqlSegment{Keyword: sqlplus.NOT}}, sqlSegmentParams[1:]...)...)
			}
			normal.executeNot = true
		}
		if normal.lastValue != nil && normal.lastValue.GetSqlSegment() != sqlplus.AND && normal.lastValue.GetSqlSegment() != sqlplus.OR && !normal.IsEmpty() {
			normal.sqlSegments = append(normal.sqlSegments, &sqlplus.SqlSegment{Keyword: sqlplus.AND})
		}
		if firstSegment.GetSqlSegment() == sqlplus.APPLY {
			sqlSegmentParams = sqlSegmentParams[1:]
		}
	}
	return true, sqlSegmentParams
}

func (normal *NormalSegmentList) RemoveAndFlushLast() {
	normal.sqlSegments = append(normal.sqlSegments[:len(normal.sqlSegments)-1])
	normal.flushLastValueByList(normal.sqlSegments)
}

func (normal *NormalSegmentList) flushLastValueByList(sqlSegmentParams []sqlplus.ISqlSegment) {
	normal.lastValue = sqlSegmentParams[len(sqlSegmentParams)-1]
}
