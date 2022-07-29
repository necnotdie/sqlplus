package mapper

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"sqlplus"
	"sqlplus/wrapper"
)

type BaseMapper struct {
	ctx  context.Context
	conn sqlx.SqlConn
	sqlplus.Mapper
}

func NewBaseMapper(ctx context.Context, conn sqlx.SqlConn) sqlplus.Mapper {
	return &BaseMapper{
		ctx:  ctx,
		conn: conn,
	}
}

func (base *BaseMapper) SelectPage(query sqlplus.Query, page sqlplus.Page) (int64, any, error) {
	count, err := base.SelectCount(query)
	if err != nil {
		return 0, nil, err
	}
	if count > 0 {
		query.Limit(true, int(page.PageSize))
		query.Offset(true, int((page.PageNum-1)*page.PageSize))
		err := base.conn.QueryRowsCtx(base.ctx, query.GetEntity(), query.GetSql(), query.GetParamPairs()...)
		return count, query.GetEntity(), err
	} else {
		return count, query.GetEntity(), err
	}
}

func (base *BaseMapper) SelectCount(query sqlplus.Query) (int64, error) {
	countWrapper := wrapper.NewQueryWrapper(nil, fmt.Sprintf("(%s) as c", query.GetSql()))
	countWrapper.Select("count(1)")
	var total int64
	err := base.conn.QueryRowCtx(base.ctx, &total, countWrapper.GetSql(), query.GetParamPairs()...)
	return total, err
}

func (base *BaseMapper) SelectList(query sqlplus.Query) (any, error) {
	err := base.conn.QueryRowsCtx(base.ctx, query.GetEntity(), query.GetSql(), query.GetParamPairs()...)
	return query.GetEntity(), err
}

func (base *BaseMapper) SelectOne(query sqlplus.Query) (any, error) {
	err := base.conn.QueryRowCtx(base.ctx, query.GetEntity(), query.GetSql(), query.GetParamPairs()...)
	return query.GetEntity(), err
}

func (base *BaseMapper) Update(update sqlplus.Update) (sql.Result, error) {
	return base.conn.ExecCtx(base.ctx, update.GetSql(), update.GetParamPairs()...)
}

func (base *BaseMapper) Delete(delete sqlplus.Delete) (sql.Result, error) {
	return base.conn.ExecCtx(base.ctx, delete.GetSql(), delete.GetParamPairs()...)
}

func (base *BaseMapper) Insert(insert sqlplus.Insert) (sql.Result, error) {
	return base.conn.ExecCtx(base.ctx, insert.GetSql(), insert.GetParamPairs()...)
}
