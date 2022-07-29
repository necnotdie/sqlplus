package segments

import (
	"sqlplus"
)

type MergeSegments struct {
	sqlplus.ISqlSegment
	normal          sqlplus.ISqlSegment
	groupBy         sqlplus.ISqlSegment
	having          sqlplus.ISqlSegment
	orderBy         sqlplus.ISqlSegment
	limit           sqlplus.ISqlSegment
	offset          sqlplus.ISqlSegment
	cacheSqlSegment bool
	sqlSegment      string
	where           string
}

func NewMergeSegments() *MergeSegments {
	return &MergeSegments{
		normal: &NormalSegmentList{
			executeNot:     true,
			flushLastValue: true,
		},
		groupBy:         new(GroupBySegmentList),
		having:          new(HavingSegmentList),
		orderBy:         new(OrderBySegmentList),
		limit:           new(LimitSegmentList),
		offset:          new(OffsetSegmentList),
		cacheSqlSegment: true,
		where:           "",
	}
}

func (merge *MergeSegments) Add(sqlSegmentParams ...sqlplus.ISqlSegment) {
	firstSqlSegment := sqlSegmentParams[0]
	switch firstSqlSegment.GetSqlSegment() {
	case sqlplus.ORDER_BY:
		merge.orderBy.Add(sqlSegmentParams...)
		break
	case sqlplus.GROUP_BY:
		merge.groupBy.Add(sqlSegmentParams...)
		break
	case sqlplus.HAVING:
		merge.having.Add(sqlSegmentParams...)
		break
	case sqlplus.LIMIT:
		merge.limit.Add(sqlSegmentParams...)
		break
	case sqlplus.OFFSET:
		merge.offset.Add(sqlSegmentParams...)
		break
	default:
		if merge.where == "" {
			merge.where = sqlplus.WHERE
		}
		merge.normal.Add(sqlSegmentParams...)
	}
	merge.cacheSqlSegment = false
}

func (merge *MergeSegments) GetSqlSegment() string {
	if merge.cacheSqlSegment {
		return merge.sqlSegment
	}
	if merge.normal.IsEmpty() {
		if !merge.groupBy.IsEmpty() || !merge.orderBy.IsEmpty() {
			merge.sqlSegment = merge.groupBy.GetSqlSegment() +
				merge.having.GetSqlSegment() +
				merge.orderBy.GetSqlSegment() +
				merge.limit.GetSqlSegment() +
				merge.offset.GetSqlSegment()
		}
	} else {
		merge.sqlSegment = merge.normal.GetSqlSegment() +
			merge.groupBy.GetSqlSegment() +
			merge.having.GetSqlSegment() +
			merge.orderBy.GetSqlSegment() +
			merge.limit.GetSqlSegment() +
			merge.offset.GetSqlSegment()
	}
	return merge.sqlSegment
}

func (merge *MergeSegments) GetWhere() string {
	return merge.where
}
