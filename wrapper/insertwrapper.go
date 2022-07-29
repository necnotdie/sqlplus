package wrapper

import (
	"fmt"
	"reflect"
	"sqlplus"
	"strings"
)

func NewInsertWrapper(table any, tabName string) *InsertWrapper {
	paramNameValuePairs := make(map[string]any)
	var insert *InsertWrapper
	tableType := sqlplus.ConvertToType(table)
	tableColumns := make(map[string]string)
	insert = &InsertWrapper{
		Entity:              table,
		TabType:             tableType,
		TabName:             tabName,
		TabColumns:          tableColumns,
		paramNameValuePairs: &paramNameValuePairs,
	}
	if tableType != nil && table != nil {
		tableValue := reflect.ValueOf(table)
		if tableValue.Kind() == reflect.Ptr {
			tableValue = tableValue.Elem()
		}
		if tableValue.Kind() == reflect.Map {
			for _, key := range tableValue.MapKeys() {
				switch key.Interface().(type) {
				case string:
					keyStr := key.Interface().(string)
					if keyStr != "" {
						paramNameValuePairs[keyStr] = tableValue.MapIndex(key).Interface()
					}
				}
			}
		}
		if tableValue.Kind() == reflect.Struct {
			for i := 0; i < insert.TabType.NumField(); i++ {
				field := insert.TabType.Field(i)
				dbName := field.Tag.Get("db")
				if dbName != "" {
					paramNameValuePairs[dbName] = tableValue.Field(i).Interface()
					insert.TabColumns[field.Name] = dbName
				}
			}
		}
	}
	return insert
}

type InsertWrapper struct {
	Entity              any
	TabType             reflect.Type
	TabName             string
	TabColumns          map[string]string
	ignoreColumns       []string
	paramNameValuePairs *map[string]any
	sqlplus.Insert
}

func (insert *InsertWrapper) GetSql() string {
	originSql := insert.GetOriginSql()
	seq := 0
	sql := sqlplus.FormatSql(originSql, func(paramName string) string {
		seq++
		return fmt.Sprintf("$%d", seq)
	})
	return sql
}

func (insert *InsertWrapper) GetOriginSql() string {
	var tableNameArray []string
	var tableValueArray []string
	for tableName, _ := range *insert.paramNameValuePairs {
		if insert.needColumn(tableName) {
			tableNameArray = append(tableNameArray, tableName)
			tableValueArray = append(tableValueArray, fmt.Sprintf("@%s", tableName))
		}
	}
	tableNameString := strings.Join(tableNameArray, ",")
	tableValueString := strings.Join(tableValueArray, ",")
	return fmt.Sprintf(sqlplus.INSERT_SQL, insert.TabName, tableNameString, tableValueString)
}

func (insert *InsertWrapper) Ignore(condition bool, column string) sqlplus.Insert {
	if condition {
		insert.ignoreColumns = append(insert.ignoreColumns, insert.columnToString(column))
	}
	return insert
}

func (insert *InsertWrapper) GetParamPairs() []any {
	var paramPairs []any
	sqlplus.FormatSql(insert.GetOriginSql(), func(name string) string {
		for paramName, paramValue := range *insert.paramNameValuePairs {
			if name == paramName {
				paramPairs = append(paramPairs, paramValue)
				break
			}
		}
		return name
	})
	return paramPairs
}

func (insert *InsertWrapper) GetParamNameValuePairs() map[string]any {
	pairs := make(map[string]any)
	for tableName, tableValue := range *insert.paramNameValuePairs {
		if insert.needColumn(tableName) {
			pairs[tableName] = tableValue
		}
	}
	return pairs
}

func (insert *InsertWrapper) needColumn(column string) bool {
	for _, ignoreColumn := range insert.ignoreColumns {
		if ignoreColumn == column {
			return false
		}
	}
	return true
}

func (insert *InsertWrapper) columnToString(columns ...string) string {
	str := new(strings.Builder)
	for i, column := range columns {
		for field, tab := range insert.TabColumns {
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
