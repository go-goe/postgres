package postgres

import (
	"fmt"
	"strings"

	"github.com/olauro/goe"
)

func buildSql(query goe.Query) (string, []any) {
	switch query.Type {
	case goe.SelectQuery:
		return buildSelect(query)
	case goe.InsertQuery:
		return buildInsert(query)
	case goe.UpdateQuery:
		return buildUpdate(query)
	case goe.DeleteQuery:
		return buildDelete(query)
		//TODO add goe.RawQuery
	}

	return "", nil
}

func buildSelect(query goe.Query) (string, []any) {
	builder := strings.Builder{}
	//TODO: check this
	args := make([]any, 0, len(query.Arguments))

	builder.WriteString("SELECT")

	builder.WriteString(writeAttributes(query.Attributes[0]))
	for _, a := range query.Attributes[1:] {
		builder.WriteByte(',')
		builder.WriteString(writeAttributes(a))
	}

	builder.WriteString("FROM")
	builder.WriteString(query.Tables[0])
	for _, t := range query.Tables[1:] {
		builder.WriteByte(',')
		builder.WriteString(t)
	}

	if query.Joins != nil {
		builder.WriteByte('\n')
		for _, j := range query.Joins {
			builder.WriteString(fmt.Sprintf("%v %v on (%v = %v)",
				j.JoinOperation,
				j.Table,
				(j.FirstArgument.Table + "." + j.FirstArgument.Name),
				(j.SecondArgument.Table + "." + j.SecondArgument.Name),
			))
		}
	}

	//TODO: create a function
	if query.WhereOperations != nil {
		builder.WriteByte('\n')
		builder.WriteString("WHERE")

		for _, w := range query.WhereOperations {
			switch w.Type {
			case goe.OperationWhere:
				builder.WriteString(fmt.Sprintf("%v %v %v", writeAttributes(w.Attribute), w.Operator, w.ValueFlag))
				args = append(args, w.Value)
			case goe.OperationIsWhere:
				builder.WriteString(fmt.Sprintf("%v %v NULL", writeAttributes(w.Attribute), w.Operator))
			case goe.OperationArgumentWhere:
				builder.WriteString(fmt.Sprintf("%v %v %v", writeAttributes(w.Attribute), w.Operator, w.ValueFlag))
			case goe.LogicalWhere:
				builder.WriteString(fmt.Sprintf(" %v ", w.Operator))
			}
		}
	}

	if query.OrderBy != nil {
		builder.WriteByte('\n')
		if query.OrderBy.Desc {
			builder.WriteString("ORDER BY" + query.OrderBy.Attribute.Table + "." + query.OrderBy.Attribute.Name + "DESC")
		} else {
			builder.WriteString("ORDER BY" + query.OrderBy.Attribute.Table + "." + query.OrderBy.Attribute.Name + "ASC")
		}
	}

	if query.Limit != 0 {
		builder.WriteByte('\n')
		builder.WriteString(fmt.Sprintf("LIMIT %v", query.Limit))
	}
	if query.Offset != 0 {
		builder.WriteByte('\n')
		builder.WriteString(fmt.Sprintf("OFFSET %v", query.Offset))
	}

	return builder.String(), args
}

func writeAttributes(a goe.Attribute) string {
	switch a.FunctionType {
	case goe.UpperFunction:
		return fmt.Sprintf(" UPPER(%v)", a.Table+"."+a.Name)
	}
	switch a.AggregateType {
	case goe.CountAggregate:
		return fmt.Sprintf(" COUNT(%v)", a.Table+"."+a.Name)

	}
	return a.Table + "." + a.Name
}

func buildInsert(query goe.Query) (string, []any) {
	builder := strings.Builder{}

	builder.WriteString("INSERT INTO")
	builder.WriteString(query.Tables[0])
	builder.WriteByte('(')

	builder.WriteString(query.Attributes[0].Name)
	for _, att := range query.Attributes[1:] {
		builder.WriteByte(',')
		builder.WriteString(att.Name)
	}
	builder.WriteString(")VALUES(")

	i := 1
	builder.WriteString(fmt.Sprintf("$%v", i))

	// TODO: check this
	for range query.SizeArguments - 1 {
		i++
		builder.WriteByte(',')
		builder.WriteString(fmt.Sprintf("$%v", i))
	}
	builder.WriteByte(')')

	for range query.BatchSizeQuery - 1 {
		i++
		builder.WriteString(",(")
		builder.WriteString(fmt.Sprintf("$%v", i))

		// TODO: check this
		for range query.SizeArguments - 1 {
			i++
			builder.WriteByte(',')
			builder.WriteString(fmt.Sprintf("$%v", i))
		}
		builder.WriteByte(')')
	}

	if query.ReturningId != nil {
		builder.WriteString("RETURNING")
		builder.WriteString(query.ReturningId.Name)
	}

	return builder.String(), query.Arguments
}

func buildUpdate(query goe.Query) (string, []any) {
	builder := strings.Builder{}
	args := make([]any, 0, len(query.Arguments))

	builder.WriteString("UPDATE")
	builder.WriteString(query.Tables[0])
	builder.WriteString("SET")

	i := 1
	builder.WriteString(query.Attributes[0].Name)
	builder.WriteByte('=')
	builder.WriteString(fmt.Sprintf("$%v", i))
	for _, att := range query.Attributes[1:] {
		i++
		builder.WriteByte(',')
		builder.WriteString(att.Name)
		builder.WriteByte('=')
		builder.WriteString(fmt.Sprintf("$%v", i))
	}

	if query.WhereOperations != nil {
		builder.WriteByte('\n')
		builder.WriteString("WHERE")

		for _, w := range query.WhereOperations {
			switch w.Type {
			case goe.OperationWhere:
				builder.WriteString(fmt.Sprintf("%v %v %v", writeAttributes(w.Attribute), w.Operator, w.ValueFlag))
				args = append(args, w.Value)
			case goe.OperationIsWhere:
				builder.WriteString(fmt.Sprintf("%v %v NULL", writeAttributes(w.Attribute), w.Operator))
			case goe.OperationArgumentWhere:
				builder.WriteString(fmt.Sprintf("%v %v %v", writeAttributes(w.Attribute), w.Operator, w.ValueFlag))
			case goe.LogicalWhere:
				builder.WriteString(fmt.Sprintf(" %v ", w.Operator))
			}
		}
	}

	query.Arguments = append(query.Arguments, args...)
	return builder.String(), query.Arguments
}

func buildDelete(query goe.Query) (string, []any) {
	builder := strings.Builder{}

	builder.WriteString("DELETE FROM")
	builder.WriteString(query.Tables[0])
	if query.WhereOperations != nil {
		builder.WriteByte('\n')
		builder.WriteString("WHERE")

		for _, w := range query.WhereOperations {
			switch w.Type {
			case goe.OperationWhere:
				builder.WriteString(fmt.Sprintf("%v %v %v", writeAttributes(w.Attribute), w.Operator, w.ValueFlag))
			case goe.OperationIsWhere:
				builder.WriteString(fmt.Sprintf("%v %v NULL", writeAttributes(w.Attribute), w.Operator))
			case goe.OperationArgumentWhere:
				builder.WriteString(fmt.Sprintf("%v %v %v", writeAttributes(w.Attribute), w.Operator, w.ValueFlag))
			case goe.LogicalWhere:
				builder.WriteString(fmt.Sprintf(" %v ", w.Operator))
			}
		}
	}

	return builder.String(), query.Arguments
}
