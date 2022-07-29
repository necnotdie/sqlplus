package wrapper

import (
	"fmt"
	"reflect"
	"sqlplus"
	"sqlplus/segments"
	"strings"
)

func NewUpdateWrapper(table any, tabName string) *UpdateWrapper {
	var seq int64 = 0
	paramNameValuePairs := make(map[string]any)
	var update *UpdateWrapper
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
	update = &UpdateWrapper{
		Entity:              table,
		TabType:             tableType,
		TabName:             tabName,
		TabColumns:          tableColumns,
		paramNameValuePairs: &paramNameValuePairs,
		expression:          mergeSegments,
		paramNameSeq:        &seq,
		updateSets: &UpdateSets{
			init:  false,
			pairs: make(map[string]any),
		},
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
	return update
}

type UpdateSets struct {
	init  bool
	pairs map[string]any
}

type UpdateWrapper struct {
	Entity              any
	TabType             reflect.Type
	TabName             string
	TabColumns          map[string]string
	paramNameValuePairs *map[string]any
	expression          *segments.MergeSegments
	paramNameSeq        *int64
	sqlSet              []string
	ignoreColumns       []string
	updateSets          *UpdateSets
	lastSql             sqlplus.ISqlSegment
	query               *QueryWrapper
	sqlplus.Update
}

func (update *UpdateWrapper) GetEntity() any {
	return update.Entity
}

func (update *UpdateWrapper) GetSqlSet() string {
	return strings.Join(update.sqlSet, sqlplus.COMMA)
}

func (update *UpdateWrapper) SetAll(condition bool) sqlplus.Update {
	if condition && update.TabType != nil && update.Entity != nil {
		tableValue := reflect.ValueOf(update.Entity)
		if tableValue.Kind() == reflect.Ptr {
			tableValue = tableValue.Elem()
		}
		if tableValue.Kind() == reflect.Map {
			for _, key := range tableValue.MapKeys() {
				switch key.Interface().(type) {
				case string:
					keyStr := key.Interface().(string)
					if keyStr != "" {
						update.updateSets.pairs[keyStr] = tableValue.MapIndex(key).Interface()
					}
				}
			}
		}
		if tableValue.Kind() == reflect.Struct {
			for i := 0; i < update.TabType.NumField(); i++ {
				dbName := update.TabType.Field(i).Tag.Get("db")
				if dbName != "" {
					update.updateSets.pairs[dbName] = tableValue.Field(i).Interface()
				}
			}
		}
	}
	return update
}

func (update *UpdateWrapper) Set(condition bool, column string, value any) sqlplus.Update {
	if condition {
		for field, tab := range update.TabColumns {
			if column == field {
				column = tab
				break
			}
		}
		update.sqlSet = append(update.sqlSet, fmt.Sprintf("%s=%s", column, update.query.FormatSql("{0}", value)))
	}
	return update
}

func (update *UpdateWrapper) SetSql(condition bool, sqls ...string) sqlplus.Update {
	if condition {
		update.sqlSet = append(update.sqlSet, sqls...)
	}
	return update
}

func (update *UpdateWrapper) GetTable() string {
	return update.TabName
}
func (update *UpdateWrapper) GetWhere() string {
	return update.query.GetWhere()
}
func (update *UpdateWrapper) GetParamPairs() []any {
	var paramPairs []any
	sqlplus.FormatSql(update.GetOriginSql(), func(name string) string {
		for paramName, paramValue := range *update.paramNameValuePairs {
			if name == paramName {
				paramPairs = append(paramPairs, paramValue)
				break
			}
		}
		return name
	})
	return paramPairs
}
func (update *UpdateWrapper) GetParamNameValuePairs() map[string]any {
	return update.query.GetParamNameValuePairs()
}
func (update *UpdateWrapper) GetSql() string {
	originSql := update.GetOriginSql()
	seq := 0
	sql := sqlplus.FormatSql(originSql, func(paramName string) string {
		seq++
		return fmt.Sprintf("$%d", seq)
	})
	return sql
}
func (update *UpdateWrapper) GetOriginSql() string {
	if !update.updateSets.init {
		update.updateSets.init = true
		for tableName, tableValue := range update.updateSets.pairs {
			if update.needColumn(tableName) {
				update.Set(true, tableName, tableValue)
			}
		}
	}
	originSql := fmt.Sprintf(sqlplus.UPDATE_SQL, update.GetTable(), update.GetSqlSet(), update.GetWhere(), update.GetSqlSegment())
	return originSql
}
func (update *UpdateWrapper) AllEq(condition bool, params map[string]any, null2IsNull bool) sqlplus.Update {
	update.query.AllEq(condition, params, null2IsNull)
	return update
}
func (update *UpdateWrapper) Eq(condition bool, column string, val any) sqlplus.Update {
	update.query.Eq(condition, column, val)
	return update
}
func (update *UpdateWrapper) Ne(condition bool, column string, val any) sqlplus.Update {
	update.query.Ne(condition, column, val)
	return update
}
func (update *UpdateWrapper) Gt(condition bool, column string, val any) sqlplus.Update {
	update.query.Gt(condition, column, val)
	return update
}
func (update *UpdateWrapper) Ge(condition bool, column string, val any) sqlplus.Update {
	update.query.Ge(condition, column, val)
	return update
}
func (update *UpdateWrapper) Le(condition bool, column string, val any) sqlplus.Update {
	update.query.Le(condition, column, val)
	return update
}
func (update *UpdateWrapper) Like(condition bool, column string, val any) sqlplus.Update {
	update.query.Like(condition, column, val)
	return update
}
func (update *UpdateWrapper) NotLike(condition bool, column string, val any) sqlplus.Update {
	update.query.NotLike(condition, column, val)
	return update
}
func (update *UpdateWrapper) LikeLeft(condition bool, column string, val any) sqlplus.Update {
	update.query.LikeLeft(condition, column, val)
	return update
}
func (update *UpdateWrapper) LikeRight(condition bool, column string, val any) sqlplus.Update {
	update.query.LikeRight(condition, column, val)
	return update
}
func (update *UpdateWrapper) Between(condition bool, column string, val1 any, val2 any) sqlplus.Update {
	update.query.Between(condition, column, val1, val2)
	return update
}
func (update *UpdateWrapper) NotBetween(condition bool, column string, val1 any, val2 any) sqlplus.Update {
	update.query.NotBetween(condition, column, val1, val2)
	return update
}
func (update *UpdateWrapper) IsNull(condition bool, column string) sqlplus.Update {
	update.query.IsNull(condition, column)
	return update
}
func (update *UpdateWrapper) IsNotNull(condition bool, column string) sqlplus.Update {
	update.query.IsNotNull(condition, column)
	return update
}
func (update *UpdateWrapper) Exists(condition bool, column string) sqlplus.Update {
	update.query.Exists(condition, column)
	return update
}
func (update *UpdateWrapper) NotExists(condition bool, column string) sqlplus.Update {
	update.query.NotExists(condition, column)
	return update
}
func (update *UpdateWrapper) In(condition bool, column string, value ...any) sqlplus.Update {
	update.query.In(condition, column, value...)
	return update
}
func (update *UpdateWrapper) NotIn(condition bool, column string, value ...any) sqlplus.Update {
	update.query.NotIn(condition, column, value...)
	return update
}
func (update *UpdateWrapper) InSql(condition bool, column string, inValue string) sqlplus.Update {
	update.query.InSql(condition, column, inValue)
	return update
}
func (update *UpdateWrapper) NotInSql(condition bool, column string, inValue string) sqlplus.Update {
	update.query.NotInSql(condition, column, inValue)
	return update
}
func (update *UpdateWrapper) AndNested(condition bool, nested sqlplus.ISqlSegment) sqlplus.Update {
	update.query.AndNested(condition, nested)
	return update
}
func (update *UpdateWrapper) OrNested(condition bool, nested sqlplus.ISqlSegment) sqlplus.Update {
	update.query.AndNested(condition, nested)
	return update
}
func (update *UpdateWrapper) Or(condition bool) sqlplus.Update {
	update.query.Or(condition)
	return update
}
func (update *UpdateWrapper) And(condition bool) sqlplus.Update {
	update.query.And(condition)
	return update
}
func (update *UpdateWrapper) Nested(condition bool, nested sqlplus.ISqlSegment) sqlplus.Update {
	update.query.Nested(condition, nested)
	return update
}
func (update *UpdateWrapper) Last(condition bool, lastSql string) sqlplus.Update {
	update.query.Last(condition, lastSql)
	return update
}

func (update *UpdateWrapper) Ignore(condition bool, column string) sqlplus.Update {
	if condition {
		update.ignoreColumns = append(update.ignoreColumns, update.columnToString(column))
	}
	return update
}

func (update *UpdateWrapper) Instance() sqlplus.Query {
	return update.query.Instance()
}

func (update *UpdateWrapper) GetSqlSegment() string {
	return update.query.GetSqlSegment()
}

func (update *UpdateWrapper) needColumn(column string) bool {
	for _, ignoreColumn := range update.ignoreColumns {
		if ignoreColumn == column {
			return false
		}
	}
	return true
}

func (update *UpdateWrapper) columnToString(columns ...string) string {
	str := new(strings.Builder)
	for i, column := range columns {
		for field, tab := range update.TabColumns {
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
