package postgres

import (
	"strconv"
	"strings"

	"github.com/go-goe/goe/enum"
	"github.com/go-goe/goe/model"
)

var joins = map[enum.JoinType]string{
	enum.Join:      "JOIN",
	enum.LeftJoin:  "LEFT JOIN",
	enum.RightJoin: "RIGHT JOIN",
}

var operators = map[enum.OperatorType]string{
	enum.And:           "AND",
	enum.Or:            "OR",
	enum.Equals:        "=",
	enum.NotEquals:     "<>",
	enum.Greater:       ">",
	enum.GreaterEquals: ">=",
	enum.Less:          "<",
	enum.LessEquals:    "<=",
	enum.Like:          "LIKE",
	enum.NotLike:       "NOT LIKE",
	enum.In:            "IN",
	enum.NotIn:         "NOT IN",
	enum.Is:            "IS",
	enum.IsNot:         "IS NOT",
}

var functions = map[enum.FunctionType]string{
	enum.UpperFunction: "UPPER",
	enum.LowerFunction: "LOWER",
}

var aggregates = map[enum.AggregateType]string{
	enum.CountAggregate: "COUNT",
	enum.MaxAggregate:   "MAX",
	enum.MinAggregate:   "MIN",
	enum.SumAggregate:   "SUM",
	enum.AvgAggregate:   "AVG",
}

func buildSql(query *model.Query) {
	switch query.Type {
	case enum.SelectQuery:
		query.RawSql = buildSelect(query)
	case enum.InsertQuery:
		query.RawSql = buildInsert(query)
	case enum.UpdateQuery:
		query.RawSql = buildUpdate(query)
	case enum.DeleteQuery:
		query.RawSql = buildDelete(query)
	}
}

func buildSelect(query *model.Query) string {
	builder := strings.Builder{}

	builder.WriteString("SELECT")

	builder.WriteString(writeAttributes(query.Attributes[0]))
	for _, a := range query.Attributes[1:] {
		builder.WriteByte(',')
		builder.WriteString(writeAttributes(a))
	}

	builder.WriteString("FROM")
	builder.WriteString(query.Tables[0].String())
	for _, t := range query.Tables[1:] {
		builder.WriteByte(',')
		builder.WriteString(t.String())
	}

	for _, j := range query.Joins {
		builder.WriteByte('\n')
		builder.WriteString(
			joins[j.JoinOperation] + " " +
				j.Table.String() + " on (" +
				(j.FirstArgument.Table + "." + j.FirstArgument.Name) + " = " +
				(j.SecondArgument.Table + "." + j.SecondArgument.Name) + ")",
		)
	}

	if query.Where != nil {
		builder.WriteByte('\n')
		builder.WriteString("WHERE")
		writeWhere(query, query.Where, &builder)
	}

	if len(query.GroupBy) != 0 {
		builder.WriteByte('\n')
		gp := query.GroupBy[0]
		builder.WriteString("GROUP BY" + writeAttributes(gp.Attribute))
		for _, gp = range query.GroupBy[1:] {
			builder.WriteString("," + writeAttributes(gp.Attribute))
		}
	}

	if len(query.OrderBy) != 0 {
		builder.WriteByte('\n')
		ob := query.OrderBy[0]
		if ob.Desc {
			builder.WriteString("ORDER BY" + writeAttributes(ob.Attribute) + "DESC")
		} else {
			builder.WriteString("ORDER BY" + writeAttributes(ob.Attribute) + "ASC")
		}
		for _, ob = range query.OrderBy[1:] {
			if ob.Desc {
				builder.WriteString("," + writeAttributes(ob.Attribute) + "DESC")
			} else {
				builder.WriteString("," + writeAttributes(ob.Attribute) + "ASC")
			}
		}
	}

	if query.Limit != 0 {
		builder.WriteByte('\n')
		builder.WriteString("LIMIT " + strconv.Itoa(query.Limit))
	}
	if query.Offset != 0 {
		builder.WriteByte('\n')
		builder.WriteString("OFFSET " + strconv.Itoa(query.Offset))
	}

	if query.ForUpdate {
		builder.WriteByte('\n')
		builder.WriteString("FOR UPDATE;")
	}

	return builder.String()
}

func buildInsert(query *model.Query) string {
	builder := strings.Builder{}

	builder.WriteString("INSERT INTO")
	builder.WriteString(query.Tables[0].String())
	builder.WriteByte('(')

	builder.WriteString(query.Attributes[0].Name)
	for _, att := range query.Attributes[1:] {
		builder.WriteByte(',')
		builder.WriteString(att.Name)
	}
	builder.WriteString(")VALUES($")

	i := 1
	builder.WriteString(strconv.Itoa(i))

	for range query.SizeArguments - 1 {
		i++
		builder.WriteString(",$" + strconv.Itoa(i))
	}
	builder.WriteByte(')')

	for range query.BatchSizeQuery - 1 {
		i++
		builder.WriteString(",($" + strconv.Itoa(i))

		for range query.SizeArguments - 1 {
			i++
			builder.WriteString(",$" + strconv.Itoa(i))
		}
		builder.WriteByte(')')
	}

	if query.ReturningID != nil {
		builder.WriteString("RETURNING")
		builder.WriteString(query.ReturningID.Name)
	}

	return builder.String()
}

func buildUpdate(query *model.Query) string {
	builder := strings.Builder{}

	builder.WriteString("UPDATE")
	builder.WriteString(query.Tables[0].String())
	builder.WriteString("SET")

	i := 1
	builder.WriteString(query.Attributes[0].Name + "=$" + strconv.Itoa(i))
	for _, att := range query.Attributes[1:] {
		i++
		builder.WriteString("," + att.Name + "=$" + strconv.Itoa(i))
	}

	if query.Where.Type != 0 {
		builder.WriteByte('\n')
		builder.WriteString("WHERE")
		writeWhere(query, query.Where, &builder)
	}

	return builder.String()
}

func buildDelete(query *model.Query) string {
	builder := strings.Builder{}

	builder.WriteString("DELETE FROM")
	builder.WriteString(query.Tables[0].String())
	if query.Where.Type != 0 {
		builder.WriteByte('\n')
		builder.WriteString("WHERE")
		writeWhere(query, query.Where, &builder)
	}

	return builder.String()
}

func writeAttributes(a model.Attribute) string {
	if a.FunctionType != 0 {
		return " " + functions[a.FunctionType] + "(" + a.Table + "." + a.Name + ")"
	}

	if a.AggregateType != 0 {
		return " " + aggregates[a.AggregateType] + "(" + a.Table + "." + a.Name + ")"
	}

	return a.Table + "." + a.Name
}

func writeWhere(query *model.Query, filter *model.Where, builder *strings.Builder) {
	switch filter.Type {
	case enum.OperationWhere:
		builder.WriteString(writeAttributes(filter.Attribute) + " " + operators[filter.Operator] + " $" + strconv.Itoa(query.WhereIndex))
		query.WhereIndex++
	case enum.OperationIsWhere:
		builder.WriteString(writeAttributes(filter.Attribute) + " " + operators[filter.Operator] + " NULL")
	case enum.OperationAttributeWhere:
		builder.WriteString(writeAttributes(filter.Attribute) + " " + operators[filter.Operator] + " " + writeAttributes(filter.AttributeValue))
	case enum.OperationInWhere:
		if filter.QueryIn != nil {
			if filter.QueryIn.Arguments != nil {
				filter.QueryIn.WhereIndex = query.WhereIndex
				query.Arguments = append(query.Arguments[:filter.QueryIn.WhereIndex-1], append(filter.QueryIn.Arguments, query.Arguments[filter.QueryIn.WhereIndex-1:]...)...)
				builder.WriteString(writeAttributes(filter.Attribute) + " " + operators[filter.Operator] + " (" + buildSelect(filter.QueryIn) + ")")
				query.WhereIndex = filter.QueryIn.WhereIndex
				return
			}
			builder.WriteString(writeAttributes(filter.Attribute) + " " + operators[filter.Operator] + " (" + buildSelect(filter.QueryIn) + ")")
			return
		}
		writeWhereInArgument(filter, builder, query)
	case enum.LogicalWhere:
		builder.WriteByte('(')
		writeWhere(query, filter.FirstOperation, builder)
		builder.WriteString(" " + operators[filter.Operator] + " ")
		writeWhere(query, filter.SecondOperation, builder)
		builder.WriteByte(')')
	}
}

func writeWhereInArgument(where *model.Where, builder *strings.Builder, query *model.Query) {
	if where.SizeIn == 0 {
		return
	}
	builder.WriteString(writeAttributes(where.Attribute) + " " + operators[where.Operator] + " " + "($" + strconv.Itoa(query.WhereIndex))
	query.WhereIndex++
	for range where.SizeIn - 1 {
		builder.WriteString(",$" + strconv.Itoa(query.WhereIndex))
		query.WhereIndex++
	}
	builder.WriteByte(')')
}
