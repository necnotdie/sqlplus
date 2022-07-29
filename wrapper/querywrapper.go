package wrapper

import (
	"fmt"
	"reflect"
	"sqlplus"
	"sqlplus/segments"
	"strings"
	"sync/atomic"
)

func NewQueryWrapper(table any, tabName string) *QueryWrapper {
	var seq int64 = 0
	paramNameValuePairs := make(map[string]any)
	var query *QueryWrapper
	tableType := sqlplus.ConvertToType(table)
	query = &QueryWrapper{
		Entity:              table,
		TabType:             tableType,
		TabName:             tabName,
		TabColumns:          make(map[string]string),
		lastSql:             new(sqlplus.SqlSegment),
		paramNameValuePairs: &paramNameValuePairs,
		expression:          segments.NewMergeSegments(),
		paramNameSeq:        &seq,
		sqlSelect:           "*",
	}
	if tableType != nil {
		for i := 0; i < query.TabType.NumField(); i++ {
			field := query.TabType.Field(i)
			query.TabColumns[field.Name] = field.Tag.Get("db")
		}
	}
	return query
}

type QueryWrapper struct {
	sqlSelect           string
	Entity              any
	TabType             reflect.Type
	TabName             string
	TabColumns          map[string]string
	paramNameValuePairs *map[string]any
	expression          *segments.MergeSegments
	paramNameSeq        *int64
	lastSql             sqlplus.ISqlSegment
	sqlplus.Query
}

func (query *QueryWrapper) GetEntity() any {
	return query.Entity
}

func (query *QueryWrapper) Select(columns ...string) sqlplus.Query {
	if len(columns) <= 0 {
		return query
	}
	query.sqlSelect = strings.Join(columns, ",")
	return query
}

func (query *QueryWrapper) GetSqlSelect() string {
	return query.sqlSelect
}

func (query *QueryWrapper) GetTable() string {
	return query.TabName
}

func (query *QueryWrapper) GetWhere() string {
	return query.expression.GetWhere()
}

func (query *QueryWrapper) GetParamPairs() []any {
	var paramPairs []any
	sqlplus.FormatSql(query.GetOriginSql(), func(name string) string {
		for paramName, paramValue := range *query.paramNameValuePairs {
			if name == paramName {
				paramPairs = append(paramPairs, paramValue)
				break
			}
		}
		return name
	})
	return paramPairs
}

func (query *QueryWrapper) GetParamNameValuePairs() map[string]any {
	return *query.paramNameValuePairs
}

func (query *QueryWrapper) GetSql() string {
	originSql := query.GetOriginSql()
	seq := 0
	sql := sqlplus.FormatSql(originSql, func(paramName string) string {
		seq++
		return fmt.Sprintf("$%d", seq)
	})
	return sql
}

func (query *QueryWrapper) GetOriginSql() string {
	originSql := fmt.Sprintf(sqlplus.SELECT_SQL, query.GetSqlSelect(), query.GetTable(), query.GetWhere(), query.GetSqlSegment())
	return originSql
}

func (query *QueryWrapper) AllEq(condition bool, params map[string]any, null2IsNull bool) sqlplus.Query {
	if condition && len(params) > 0 {
		for key, value := range params {
			if value != nil {
				query.Eq(true, key, value)
			} else {
				if null2IsNull {
					query.IsNull(true, key)
				}
			}
		}
	}
	return query
}

func (query *QueryWrapper) Eq(condition bool, column string, val any) sqlplus.Query {
	return query.addCondition(condition, column, sqlplus.EQ, val)
}

func (query *QueryWrapper) Ne(condition bool, column string, val any) sqlplus.Query {
	return query.addCondition(condition, column, sqlplus.NE, val)
}

func (query *QueryWrapper) Gt(condition bool, column string, val any) sqlplus.Query {
	return query.addCondition(condition, column, sqlplus.GT, val)
}
func (query *QueryWrapper) Ge(condition bool, column string, val any) sqlplus.Query {
	return query.addCondition(condition, column, sqlplus.GE, val)
}
func (query *QueryWrapper) Le(condition bool, column string, val any) sqlplus.Query {
	return query.addCondition(condition, column, sqlplus.LE, val)
}

func (query *QueryWrapper) Like(condition bool, column string, val any) sqlplus.Query {
	return query.likeValue(condition, column, val, sqlplus.SQL_LIKE)
}

func (query *QueryWrapper) NotLike(condition bool, column string, val any) sqlplus.Query {
	return query.not(condition).Like(condition, column, val)
}

func (query *QueryWrapper) LikeLeft(condition bool, column string, val any) sqlplus.Query {
	return query.likeValue(condition, column, val, sqlplus.SQL_LIKE_LEFT)
}

func (query *QueryWrapper) LikeRight(condition bool, column string, val any) sqlplus.Query {
	return query.likeValue(condition, column, val, sqlplus.SQL_LIKE_RIGHT)
}

func (query *QueryWrapper) likeValue(condition bool, column string, val any, sqlLike int) sqlplus.Query {
	return query.doIt(condition, &sqlplus.SqlSegment{Keyword: query.columnToString(column)}, &sqlplus.SqlSegment{Keyword: sqlplus.LIKE}, &sqlplus.SqlSegment{Keyword: query.FormatSql("{0}", query.concatLike(val, sqlLike))})
}

func (query *QueryWrapper) Between(condition bool, column string, val1 any, val2 any) sqlplus.Query {
	return query.doIt(condition,
		&sqlplus.SqlSegment{Keyword: query.columnToString(column)},
		&sqlplus.SqlSegment{Keyword: sqlplus.BETWEEN},
		&sqlplus.SqlSegment{Keyword: query.FormatSql("{0}", val1)},
		&sqlplus.SqlSegment{Keyword: sqlplus.AND},
		&sqlplus.SqlSegment{Keyword: query.FormatSql("{0}", val2)})
}

func (query *QueryWrapper) NotBetween(condition bool, column string, val1 any, val2 any) sqlplus.Query {
	return query.not(condition).Between(condition, column, val1, val2)
}

func (query *QueryWrapper) IsNull(condition bool, column string) sqlplus.Query {
	return query.doIt(condition, &sqlplus.SqlSegment{Keyword: query.columnToString(column)}, &sqlplus.SqlSegment{Keyword: sqlplus.IS_NULL})
}

func (query *QueryWrapper) IsNotNull(condition bool, column string) sqlplus.Query {
	return query.doIt(condition, &sqlplus.SqlSegment{Keyword: query.columnToString(column)}, &sqlplus.SqlSegment{Keyword: sqlplus.IS_NOT_NULL})
}

func (query *QueryWrapper) Exists(condition bool, existsSql string) sqlplus.Query {
	return query.doIt(condition,
		&sqlplus.SqlSegment{Keyword: sqlplus.EXISTS},
		&sqlplus.SqlSegment{Keyword: fmt.Sprintf("(%s)", existsSql)})
}

func (query *QueryWrapper) NotExists(condition bool, existsSql string) sqlplus.Query {
	return query.not(condition).Exists(condition, existsSql)
}

func (query *QueryWrapper) In(condition bool, column string, value ...any) sqlplus.Query {
	if len(value) <= 0 {
		return query
	}
	return query.doIt(condition,
		&sqlplus.SqlSegment{Keyword: column},
		&sqlplus.SqlSegment{Keyword: sqlplus.IN},
		&sqlplus.SqlSegment{Keyword: query.inExpression(value...)})
}

func (query *QueryWrapper) NotIn(condition bool, column string, value ...any) sqlplus.Query {
	if len(value) <= 0 {
		return query
	}
	return query.not(condition).In(condition, column, value...)
}

func (query *QueryWrapper) InSql(condition bool, column string, inValue string) sqlplus.Query {
	return query.doIt(condition,
		&sqlplus.SqlSegment{Keyword: column},
		&sqlplus.SqlSegment{Keyword: sqlplus.IN},
		&sqlplus.SqlSegment{Keyword: fmt.Sprintf("(%s)", inValue)})
}

func (query *QueryWrapper) NotInSql(condition bool, column string, inValue string) sqlplus.Query {
	return query.not(condition).InSql(condition, column, inValue)
}

func (query *QueryWrapper) AndNested(condition bool, nested sqlplus.ISqlSegment) sqlplus.Query {
	return query.And(condition).Nested(condition, nested)
}

func (query *QueryWrapper) OrNested(condition bool, nested sqlplus.ISqlSegment) sqlplus.Query {
	return query.Or(condition).Nested(condition, nested)
}

func (query *QueryWrapper) Nested(condition bool, nested sqlplus.ISqlSegment) sqlplus.Query {
	return query.addNestedCondition(condition, nested)
}

func (query *QueryWrapper) GroupBy(condition bool, columns ...string) sqlplus.Query {
	if len(columns) <= 0 {
		return query
	}
	return query.doIt(condition,
		&sqlplus.SqlSegment{Keyword: sqlplus.GROUP_BY},
		&sqlplus.SqlSegment{Keyword: query.columnToString(columns...)})
}
func (query *QueryWrapper) OrderByAsc(condition bool, columns ...string) sqlplus.Query {
	if len(columns) <= 0 {
		return query
	}
	return query.doIt(condition,
		&sqlplus.SqlSegment{Keyword: sqlplus.ORDER_BY},
		&sqlplus.SqlSegment{Keyword: query.columnToString(columns...)},
		&sqlplus.SqlSegment{Keyword: sqlplus.ASC})
}
func (query *QueryWrapper) OrderByDesc(condition bool, columns ...string) sqlplus.Query {
	if len(columns) <= 0 {
		return query
	}
	return query.doIt(condition,
		&sqlplus.SqlSegment{Keyword: sqlplus.ORDER_BY},
		&sqlplus.SqlSegment{Keyword: query.columnToString(columns...)},
		&sqlplus.SqlSegment{Keyword: sqlplus.DESC})
}
func (query *QueryWrapper) Having(condition bool, sqlHaving string, params ...any) sqlplus.Query {
	return query.doIt(condition,
		&sqlplus.SqlSegment{Keyword: sqlplus.HAVING},
		&sqlplus.SqlSegment{Keyword: query.FormatSqlIfNeed(condition, sqlHaving, params)})
}

func (query *QueryWrapper) Limit(condition bool, num int, values ...int) sqlplus.Query {
	if len(values) > 0 {
		return query.doIt(condition,
			&sqlplus.SqlSegment{Keyword: sqlplus.LIMIT},
			&sqlplus.SqlSegment{Keyword: query.FormatSql("{0}", num)},
			&sqlplus.SqlSegment{Keyword: query.FormatSql("{0}", values[0])})
	} else {
		return query.doIt(condition,
			&sqlplus.SqlSegment{Keyword: sqlplus.LIMIT},
			&sqlplus.SqlSegment{Keyword: query.FormatSql("{0}", num)})
	}
}

func (query *QueryWrapper) Offset(condition bool, num int) sqlplus.Query {
	return query.doIt(condition,
		&sqlplus.SqlSegment{Keyword: sqlplus.OFFSET},
		&sqlplus.SqlSegment{Keyword: query.FormatSql("{0}", num)})
}

func (query *QueryWrapper) Last(condition bool, lastSql string) sqlplus.Query {
	if condition {
		query.lastSql.(*sqlplus.SqlSegment).Keyword = lastSql
	}
	return query
}

func (query *QueryWrapper) GetSqlSegment() string {
	sqlSegment := query.expression.GetSqlSegment()
	if sqlSegment != "" {
		return sqlSegment + query.lastSql.GetSqlSegment()
	}
	if query.lastSql.GetSqlSegment() != "" {
		return query.lastSql.GetSqlSegment()
	}
	return ""
}

func (query *QueryWrapper) Instance() sqlplus.Query {
	return &QueryWrapper{
		TabType:             query.TabType,
		TabName:             query.TabName,
		TabColumns:          query.TabColumns,
		lastSql:             new(sqlplus.SqlSegment),
		paramNameSeq:        query.paramNameSeq,
		expression:          segments.NewMergeSegments(),
		paramNameValuePairs: query.paramNameValuePairs,
	}
}

func (query *QueryWrapper) not(condition bool) sqlplus.Query {
	return query.doIt(condition, &sqlplus.SqlSegment{Keyword: sqlplus.NOT})
}

func (query *QueryWrapper) And(condition bool) sqlplus.Query {
	return query.doIt(condition, &sqlplus.SqlSegment{Keyword: sqlplus.AND})
}

func (query *QueryWrapper) Or(condition bool) sqlplus.Query {
	return query.doIt(condition, &sqlplus.SqlSegment{Keyword: sqlplus.OR})
}

func (query *QueryWrapper) addCondition(condition bool, column string, sqlKeyword string, val any) sqlplus.Query {
	column = query.columnToString(column)
	return query.doIt(condition,
		&sqlplus.SqlSegment{Keyword: column},
		&sqlplus.SqlSegment{Keyword: sqlKeyword},
		&sqlplus.SqlSegment{Keyword: query.FormatSql("{0}", val)})
}

func (query *QueryWrapper) addNestedCondition(condition bool, iSqlSegment sqlplus.ISqlSegment) sqlplus.Query {
	return query.doIt(condition,
		&sqlplus.SqlSegment{Keyword: sqlplus.LEFT_BRACKET},
		iSqlSegment,
		&sqlplus.SqlSegment{Keyword: sqlplus.RIGHT_BRACKET})
}

func (query *QueryWrapper) columnToString(columns ...string) string {
	str := new(strings.Builder)
	for i, column := range columns {
		for field, tab := range query.TabColumns {
			if column == field {
				column = tab
				break
			}
		}
		str.WriteString(column)
		if i < len(columns)-1 {
			str.WriteString(sqlplus.COMMA)
		}
	}
	return str.String()
}

func (query *QueryWrapper) doIt(condition bool, params ...sqlplus.ISqlSegment) sqlplus.Query {
	if condition {
		query.expression.Add(params...)
	}
	return query
}

func (query *QueryWrapper) FormatSql(sqlStr string, val ...any) string {
	return query.FormatSqlIfNeed(true, sqlStr, val...)
}

func (query *QueryWrapper) FormatSqlIfNeed(need bool, sqlStr string, val ...any) string {
	if need && len(val) > 0 {
		for i, arg := range val {
			paramName := fmt.Sprintf("arg%d", *query.paramNameSeq)
			sqlStr = strings.ReplaceAll(sqlStr, fmt.Sprintf("{%d}", i), fmt.Sprintf("@%s", paramName))
			(*query.paramNameValuePairs)[paramName] = arg
			atomic.AddInt64(query.paramNameSeq, 1)
		}
		return sqlStr
	}
	return ""
}

func (query *QueryWrapper) inExpression(value ...any) string {
	var str = new(strings.Builder)
	for i, arg := range value {
		str.WriteString(query.FormatSql("{0}", arg))
		if i < len(value)-1 {
			str.WriteString(sqlplus.COMMA)
		}
	}
	return fmt.Sprintf("%s%s%s", sqlplus.LEFT_BRACKET, str.String(), sqlplus.RIGHT_BRACKET)
}

func (query *QueryWrapper) concatLike(val any, sqlLike int) string {
	switch sqlLike {
	case sqlplus.SQL_LIKE_LEFT:
		return fmt.Sprintf("%s%v", sqlplus.PERCENT, val)
	case sqlplus.SQL_LIKE_RIGHT:
		return fmt.Sprintf("%v%s", val, sqlplus.PERCENT)
	default:
		return fmt.Sprintf("%s%v%s", sqlplus.PERCENT, val, sqlplus.PERCENT)
	}
}
