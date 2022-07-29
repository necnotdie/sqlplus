package wrapper

import (
	"fmt"
	"reflect"
	"sqlplus"
	"sqlplus/segments"
)

func NewDeleteWrapper(table any, tabName string) *DeleteWrapper {
	var seq int64 = 0
	paramNameValuePairs := make(map[string]any)
	var deleteWrapper *DeleteWrapper
	mergeSegments := segments.NewMergeSegments()
	tableColumns := make(map[string]string)
	tableType := sqlplus.ConvertToType(table)
	if tableType != nil {
		if tableType.Kind() == reflect.Struct {
			for i := 0; i < tableType.NumField(); i++ {
				field := tableType.Field(i)
				tableColumns[field.Name] = field.Tag.Get("db")
			}
		}
	}
	deleteWrapper = &DeleteWrapper{
		Entity:              table,
		TabType:             tableType,
		TabName:             tabName,
		TabColumns:          tableColumns,
		paramNameValuePairs: &paramNameValuePairs,
		expression:          mergeSegments,
		paramNameSeq:        &seq,
		query: &QueryWrapper{
			Entity:              table,
			TabType:             tableType,
			TabName:             tabName,
			TabColumns:          tableColumns,
			lastSql:             new(sqlplus.SqlSegment),
			paramNameValuePairs: &paramNameValuePairs,
			expression:          mergeSegments,
			paramNameSeq:        &seq,
			sqlSelect:           "*",
		},
	}
	return deleteWrapper
}

type DeleteWrapper struct {
	Entity              any
	TabType             reflect.Type
	TabName             string
	TabColumns          map[string]string
	paramNameValuePairs *map[string]any
	expression          *segments.MergeSegments
	paramNameSeq        *int64
	lastSql             sqlplus.ISqlSegment
	query               *QueryWrapper
	sqlplus.Delete
}

func (delete *DeleteWrapper) GetEntity() any {
	return delete.Entity
}

func (delete *DeleteWrapper) GetTable() string {
	return delete.TabName
}
func (delete *DeleteWrapper) GetWhere() string {
	return delete.query.GetWhere()
}
func (delete *DeleteWrapper) GetParamPairs() []any {
	var paramPairs []any
	sqlplus.FormatSql(delete.GetOriginSql(), func(name string) string {
		for paramName, paramValue := range *delete.paramNameValuePairs {
			if name == paramName {
				paramPairs = append(paramPairs, paramValue)
				break
			}
		}
		return name
	})
	return paramPairs
}
func (delete *DeleteWrapper) GetParamNameValuePairs() map[string]any {
	return delete.query.GetParamNameValuePairs()
}
func (delete *DeleteWrapper) GetSql() string {
	originSql := delete.GetOriginSql()
	seq := 0
	sql := sqlplus.FormatSql(originSql, func(paramName string) string {
		seq++
		return fmt.Sprintf("$%d", seq)
	})
	return sql
}
func (delete *DeleteWrapper) GetOriginSql() string {
	originSql := fmt.Sprintf(sqlplus.DELETE_SQL, delete.GetTable(), delete.GetWhere(), delete.GetSqlSegment())
	return originSql
}
func (delete *DeleteWrapper) AllEq(condition bool, params map[string]any, null2IsNull bool) sqlplus.Delete {
	delete.query.AllEq(condition, params, null2IsNull)
	return delete
}
func (delete *DeleteWrapper) Eq(condition bool, column string, val any) sqlplus.Delete {
	delete.query.Eq(condition, column, val)
	return delete
}
func (delete *DeleteWrapper) Ne(condition bool, column string, val any) sqlplus.Delete {
	delete.query.Ne(condition, column, val)
	return delete
}
func (delete *DeleteWrapper) Gt(condition bool, column string, val any) sqlplus.Delete {
	delete.query.Gt(condition, column, val)
	return delete
}
func (delete *DeleteWrapper) Ge(condition bool, column string, val any) sqlplus.Delete {
	delete.query.Ge(condition, column, val)
	return delete
}
func (delete *DeleteWrapper) Le(condition bool, column string, val any) sqlplus.Delete {
	delete.query.Le(condition, column, val)
	return delete
}
func (delete *DeleteWrapper) Like(condition bool, column string, val any) sqlplus.Delete {
	delete.query.Like(condition, column, val)
	return delete
}
func (delete *DeleteWrapper) NotLike(condition bool, column string, val any) sqlplus.Delete {
	delete.query.NotLike(condition, column, val)
	return delete
}
func (delete *DeleteWrapper) LikeLeft(condition bool, column string, val any) sqlplus.Delete {
	delete.query.LikeLeft(condition, column, val)
	return delete
}
func (delete *DeleteWrapper) LikeRight(condition bool, column string, val any) sqlplus.Delete {
	delete.query.LikeRight(condition, column, val)
	return delete
}
func (delete *DeleteWrapper) Between(condition bool, column string, val1 any, val2 any) sqlplus.Delete {
	delete.query.Between(condition, column, val1, val2)
	return delete
}
func (delete *DeleteWrapper) NotBetween(condition bool, column string, val1 any, val2 any) sqlplus.Delete {
	delete.query.NotBetween(condition, column, val1, val2)
	return delete
}
func (delete *DeleteWrapper) IsNull(condition bool, column string) sqlplus.Delete {
	delete.query.IsNull(condition, column)
	return delete
}
func (delete *DeleteWrapper) IsNotNull(condition bool, column string) sqlplus.Delete {
	delete.query.IsNotNull(condition, column)
	return delete
}
func (delete *DeleteWrapper) Exists(condition bool, column string) sqlplus.Delete {
	delete.query.Exists(condition, column)
	return delete
}
func (delete *DeleteWrapper) NotExists(condition bool, column string) sqlplus.Delete {
	delete.query.NotExists(condition, column)
	return delete
}
func (delete *DeleteWrapper) In(condition bool, column string, value ...any) sqlplus.Delete {
	delete.query.In(condition, column, value...)
	return delete
}
func (delete *DeleteWrapper) NotIn(condition bool, column string, value ...any) sqlplus.Delete {
	delete.query.NotIn(condition, column, value...)
	return delete
}
func (delete *DeleteWrapper) InSql(condition bool, column string, inValue string) sqlplus.Delete {
	delete.query.InSql(condition, column, inValue)
	return delete
}
func (delete *DeleteWrapper) NotInSql(condition bool, column string, inValue string) sqlplus.Delete {
	delete.query.NotInSql(condition, column, inValue)
	return delete
}
func (delete *DeleteWrapper) AndNested(condition bool, nested sqlplus.ISqlSegment) sqlplus.Delete {
	delete.query.AndNested(condition, nested)
	return delete
}
func (delete *DeleteWrapper) OrNested(condition bool, nested sqlplus.ISqlSegment) sqlplus.Delete {
	delete.query.AndNested(condition, nested)
	return delete
}
func (delete *DeleteWrapper) Or(condition bool) sqlplus.Delete {
	delete.query.Or(condition)
	return delete
}
func (delete *DeleteWrapper) And(condition bool) sqlplus.Delete {
	delete.query.And(condition)
	return delete
}
func (delete *DeleteWrapper) Nested(condition bool, nested sqlplus.ISqlSegment) sqlplus.Delete {
	delete.query.Nested(condition, nested)
	return delete
}
func (delete *DeleteWrapper) Last(condition bool, lastSql string) sqlplus.Delete {
	delete.query.Last(condition, lastSql)
	return delete
}

func (delete *DeleteWrapper) Instance() sqlplus.Query {
	return delete.query.Instance()
}

func (delete *DeleteWrapper) GetSqlSegment() string {
	return delete.query.GetSqlSegment()
}
