package org.mule.modules.cassandradb.internal.util;

import org.mule.runtime.extension.api.dsql.Direction;
import org.mule.runtime.extension.api.dsql.EntityType;
import org.mule.runtime.extension.api.dsql.Field;
import org.mule.runtime.extension.api.dsql.QueryTranslator;
import org.mule.runtime.extension.api.dsql.Value;

import java.util.Iterator;
import java.util.List;

public class DefaultDsqlQueryTranslator implements QueryTranslator {

    private StringBuilder queryBuilder;

    public DefaultDsqlQueryTranslator() {
        queryBuilder = new StringBuilder();
    }

    @Override
    public void translateFields(List<Field> fields) {
        StringBuilder select = new StringBuilder();
        select.append("SELECT ");
        Iterator<Field> fieldIterable = fields.iterator();
        while (fieldIterable.hasNext()) {
            String fieldName = addQuotesIfNeeded(fieldIterable.next().getName());
            select.append(fieldName);
            if (fieldIterable.hasNext()) {
                select.append(",");
            }
        }

        queryBuilder.insert(0, select);
    }

    @Override
    public void translateTypes(EntityType type) {
        queryBuilder.append(" FROM ");
        String typeName = addQuotesIfNeeded(type.getName());
        queryBuilder.append(typeName);
    }

    @Override
    public void translateOrderByFields(List<Field> orderByFields, Direction direction) {
        queryBuilder.append(" ORDER BY ");
        Iterator<Field> orderByFieldsIterator = orderByFields.iterator();
        while (orderByFieldsIterator.hasNext()) {
            String fieldName = addQuotesIfNeeded(orderByFieldsIterator.next().getName());
            queryBuilder.append(fieldName);
            if (orderByFieldsIterator.hasNext()) {
                queryBuilder.append(",");
            }
        }

        queryBuilder.append(" ");
        queryBuilder.append(direction.toString());
    }

    @Override
    public void translateBeginExpression() {
        queryBuilder.append(" WHERE ");
    }

    @Override
    public void translateInitPrecedence() {
        queryBuilder.append("(");
    }

    @Override
    public void translateEndPrecedence() {
        queryBuilder.append(")");
    }

    @Override
    public void translateLimit(int limit) {
        queryBuilder.append(" LIMIT ").append(limit);
    }

    @Override
    public void translateOffset(int offset) {
        queryBuilder.append(" OFFSET ").append(offset);
    }

    @Override
    public void translateAnd() {
        queryBuilder.append(" AND ");
    }

    @Override
    public void translateOR() {
        queryBuilder.append(" OR ");
    }

    @Override
    public void translateComparison(String operator, Field field, Value<?> value) {
        String name = addQuotesIfNeeded(field.getName());
        queryBuilder.append(name).append(operator).append(value.toString());
    }

    @Override
    public String getTranslation() {
        return queryBuilder.toString();
    }

    private String addQuotesIfNeeded(String name) {
        return name.contains(" ") ? "'" + name + "'" : name;
    }
}
