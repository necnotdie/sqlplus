package sqlplus

import (
	"database/sql"
	"reflect"
	"strings"
)

const (
	AND         = "AND"
	OR          = "OR"
	IN          = "IN"
	NOT         = "NOT"
	LIKE        = "LIKE"
	EQ          = "="
	NE          = "<>"
	GT          = ">"
	GE          = ">="
	LT          = "<"
	LE          = "<="
	IS_NULL     = "IS NULL"
	IS_NOT_NULL = "IS NOT NULL"
	GROUP_BY    = "GROUP BY"
	HAVING      = "HAVING"
	ORDER_BY    = "ORDER BY"
	EXISTS      = "EXISTS"
	BETWEEN     = "BETWEEN"
	ASC         = "ASC"
	DESC        = "DESC"
	APPLY       = "APPLY"
	WHERE       = "WHERE"
	LIMIT       = "LIMIT"
	OFFSET      = "OFFSET"
)

const (
	LEFT_BRACE    = "{"
	LEFT_BRACKET  = "("
	RIGHT_BRACE   = "}"
	RIGHT_BRACKET = ")"
	SPACE         = " "
	EMPTY         = ""
	COMMA         = ","
	PERCENT       = "%"
)

const (
	SQL_LIKE       = 0
	SQL_LIKE_LEFT  = 1
	SQL_LIKE_RIGHT = 2
)

const (
	SELECT_SQL = "SELECT %s FROM %s %s %s"
	UPDATE_SQL = "UPDATE %s SET %s %s %s"
	DELETE_SQL = "DELETE FROM %s %s %s"
	INSERT_SQL = "INSERT INTO %s (%s) values (%s)"
)

type ISqlSegment interface {
	GetSqlSegment() string
	IsEmpty() bool
	Add(...ISqlSegment)
	TransformList([]ISqlSegment, ISqlSegment) (bool, []ISqlSegment)
	RemoveAndFlushLast()
}

type SqlSegment struct {
	ISqlSegment
	Keyword string
}

func (sqlSegment *SqlSegment) GetSqlSegment() string {
	return sqlSegment.Keyword
}

type Query interface {
	GetEntity() any
	Select(...string) Query
	GetSqlSelect() string
	GetTable() string
	GetWhere() string
	GetParamPairs() []any
	GetParamNameValuePairs() map[string]any
	GetSql() string
	GetOriginSql() string
	AllEq(bool, map[string]any, bool) Query
	Eq(bool, string, any) Query
	Ne(bool, string, any) Query
	Gt(bool, string, any) Query
	Ge(bool, string, any) Query
	Le(bool, string, any) Query
	Like(bool, string, any) Query
	NotLike(bool, string, any) Query
	LikeLeft(bool, string, any) Query
	LikeRight(bool, string, any) Query
	Between(bool, string, any, any) Query
	NotBetween(bool, string, any, any) Query
	IsNull(bool, string) Query
	IsNotNull(bool, string) Query
	Exists(bool, string) Query
	NotExists(bool, string) Query
	In(bool, string, ...any) Query
	NotIn(bool, string, ...any) Query
	InSql(bool, string, string) Query
	NotInSql(bool, string, string) Query
	AndNested(bool, ISqlSegment) Query
	OrNested(bool, ISqlSegment) Query
	Or(bool) Query
	And(bool) Query
	Nested(bool, ISqlSegment) Query
	GroupBy(bool, ...string) Query
	OrderByAsc(bool, ...string) Query
	OrderByDesc(bool, ...string) Query
	Having(bool, string, ...any) Query
	Limit(bool, int, ...int) Query
	Offset(bool, int) Query
	Last(bool, string) Query
	Instance() Query
	ISqlSegment
}

type Update interface {
	GetEntity() any
	GetSqlSet() string
	SetAll(bool) Update
	Set(bool, string, any) Update
	SetSql(bool, ...string) Update
	GetTable() string
	GetWhere() string
	GetParamPairs() []any
	GetParamNameValuePairs() map[string]any
	GetSql() string
	GetOriginSql() string
	AllEq(bool, map[string]any, bool) Update
	Eq(bool, string, any) Update
	Ne(bool, string, any) Update
	Gt(bool, string, any) Update
	Ge(bool, string, any) Update
	Le(bool, string, any) Update
	Like(bool, string, any) Update
	NotLike(bool, string, any) Update
	LikeLeft(bool, string, any) Update
	LikeRight(bool, string, any) Update
	Between(bool, string, any, any) Update
	NotBetween(bool, string, any, any) Update
	IsNull(bool, string) Update
	IsNotNull(bool, string) Update
	Exists(bool, string) Update
	NotExists(bool, string) Update
	In(bool, string, ...any) Update
	NotIn(bool, string, ...any) Update
	InSql(bool, string, string) Update
	NotInSql(bool, string, string) Update
	AndNested(bool, ISqlSegment) Update
	OrNested(bool, ISqlSegment) Update
	Or(bool) Update
	And(bool) Update
	Nested(bool, ISqlSegment) Update
	Last(bool, string) Update
	Ignore(bool, string) Update
	Instance() Query
	ISqlSegment
}

type Delete interface {
	GetEntity() any
	GetTable() string
	GetWhere() string
	GetParamPairs() []any
	GetParamNameValuePairs() map[string]any
	GetSql() string
	GetOriginSql() string
	AllEq(bool, map[string]any, bool) Delete
	Eq(bool, string, any) Delete
	Ne(bool, string, any) Delete
	Gt(bool, string, any) Delete
	Ge(bool, string, any) Delete
	Le(bool, string, any) Delete
	Like(bool, string, any) Delete
	NotLike(bool, string, any) Delete
	LikeLeft(bool, string, any) Delete
	LikeRight(bool, string, any) Delete
	Between(bool, string, any, any) Delete
	NotBetween(bool, string, any, any) Delete
	IsNull(bool, string) Delete
	IsNotNull(bool, string) Delete
	Exists(bool, string) Delete
	NotExists(bool, string) Delete
	In(bool, string, ...any) Delete
	NotIn(bool, string, ...any) Delete
	InSql(bool, string, string) Delete
	NotInSql(bool, string, string) Delete
	AndNested(bool, ISqlSegment) Delete
	OrNested(bool, ISqlSegment) Delete
	Or(bool) Delete
	And(bool) Delete
	Nested(bool, ISqlSegment) Delete
	Last(bool, string) Delete
	Instance() Query
	ISqlSegment
}

type Insert interface {
	GetSql() string
	Ignore(bool, string) Insert
	GetOriginSql() string
	GetParamPairs() []any
	GetParamNameValuePairs() map[string]any
}

type Page struct {
	PageNum  int64
	PageSize int64
}

type Mapper interface {
	SelectPage(Query, Page) (int64, any, error)
	SelectCount(Query) (int64, error)
	SelectList(Query) (any, error)
	SelectOne(Query) (any, error)
	Update(Update) (sql.Result, error)
	Delete(Delete) (sql.Result, error)
	Insert(Insert) (sql.Result, error)
}

func FormatSql(originSql string, exec func(string) string) string {
	sql := new(strings.Builder)
	bytes := len(originSql)
	for i := 0; i < bytes; i++ {
		ch := originSql[i]
		switch ch {
		case '@':
			var j int
			for j = i + 1; j < bytes; j++ {
				char := originSql[j]
				if (char < '0' || char > '9') && (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char != '_') {
					break
				}
			}
			if j > i+1 {
				paramName := originSql[i+1 : j]
				sql.WriteString(exec(paramName))
				i = j - 1
			}
		default:
			sql.WriteByte(ch)
		}
	}
	return sql.String()
}

func ConvertToType(source any) reflect.Type {
	if source == nil {
		return nil
	}

	var GetTypeByType = func(value reflect.Type) reflect.Type {
		switch value.Kind() {
		case reflect.Ptr:
			return ConvertToType(value.Elem())
		case reflect.Slice:
			return ConvertToType(value.Elem())
		case reflect.Array:
			return ConvertToType(value.Elem())
		case reflect.Struct:
			return value
		case reflect.Map:
			return value
		default:
			return nil
		}
	}

	var tableType reflect.Type
	switch source.(type) {
	case reflect.Type:
		tableType = GetTypeByType(source.(reflect.Type))
		break
	case reflect.Value:
		tableType = GetTypeByType(source.(reflect.Value).Type())
		break
	default:
		tableType = GetTypeByType(reflect.TypeOf(source))
	}
	return tableType
}
