/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.utils.builders;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.*;
import com.mulesoft.mule.cassandradb.metadata.*;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import com.mulesoft.mule.cassandradb.utils.Constants;
import com.mulesoft.mule.cassandradb.utils.ReplicationStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HelperStatements {

    public static SchemaStatement createKeyspaceStatement(CreateKeyspaceInput input) {
        //build create keyspace statement if not exists
        CreateKeyspace createKeyspaceStatement = SchemaBuilder.createKeyspace(input.getKeyspaceName()).ifNotExists();

        return createKeyspaceStatement.with().replication(ReplicationStrategy.buildReplicationStrategy(input));
    }

    public static SchemaStatement dropKeyspaceStatement(String keyspaceName) {
        return SchemaBuilder.dropKeyspace(keyspaceName).ifExists();
    }

    public static SchemaStatement createTable(String keyspace, CreateTableInput input) throws CassandraDBException {

        if (input.getColumns() == null || input.getColumns().isEmpty()) {
            throw new CassandraDBException("Mismatched input. Cannot create table without columns.");
        }

        Create table = SchemaBuilder.createTable(keyspace, input.getTableName()).ifNotExists();

        List<ColumnInput> partitionKey = getPartitionKey(input.getColumns());
        if (partitionKey.isEmpty()) {
            throw new CassandraDBException("Mismatched input. Primary key is missing.");
        } else {
            for (ColumnInput column : getPartitionKey(input.getColumns())) {
                table.addPartitionKey(column.getName(), resolveDataTypeFromString(String.valueOf(column.getType())));
            }
        }
        List<ColumnInput> otherColumns = getColumnsThatAreNotPrimaryKey(input.getColumns());
        if (!otherColumns.isEmpty()) {
            for (ColumnInput column : otherColumns) {
                table.addColumn(column.getName(), ColumnType.resolveDataType(column.getType()));
            }
        }
        return table;
    }

    public static SchemaStatement addNewColumn(String tableName, String keyspaceName, String columnName, DataType columnType) {
        return SchemaBuilder.alterTable(keyspaceName, tableName).addColumn(columnName).type(columnType);
    }

    public static SchemaStatement dropColumn(String tableName, String keyspaceName, String columnName) {
        return SchemaBuilder.alterTable(keyspaceName, tableName).dropColumn(columnName);
    }

    public static SchemaStatement changeColumnType(String tableName, String keyspaceName, AlterColumnInput input) {
        return SchemaBuilder.alterTable(keyspaceName, tableName).alterColumn(input.getColumn()).type(ColumnType.resolveDataType(input.getType()));
    }

    public static SchemaStatement renameColumn(String tableName, String keyspaceName, String oldColumnName, String newColumnName) {
        return SchemaBuilder.alterTable(keyspaceName, tableName).renameColumn(oldColumnName).to(newColumnName);
    }

    public static SchemaStatement dropTable(String tableName, String keyspaceName) {
        return SchemaBuilder.dropTable(keyspaceName, tableName).ifExists();
    }

    /**
     * return the DataType based on a String. Default value is DataType.text();
     */
    public static DataType resolveDataTypeFromString(String dataType) {
        DataType.Name name = DataType.Name.valueOf(dataType.toUpperCase());
        switch (name){
            case ASCII: return DataType.ascii();
            case BIGINT: return DataType.bigint();
            case BLOB: return DataType.blob();
            case BOOLEAN: return DataType.cboolean();
            case COUNTER: return DataType.counter();
            case DECIMAL: return DataType.decimal();
            case DOUBLE: return DataType.cdouble();
            case FLOAT: return DataType.cfloat();
            case INET: return DataType.inet();
            case TINYINT: return DataType.tinyint();
            case SMALLINT: return DataType.smallint();
            case INT: return DataType.cint();
            case TEXT: return DataType.text();
            case TIMESTAMP: return DataType.timestamp();
            case DATE: return DataType.date();
            case TIME: return DataType.time();
            case UUID: return DataType.uuid();
            case VARCHAR: return DataType.varchar();
            case VARINT: return DataType.varint();
            case TIMEUUID: return DataType.timeuuid();
            default: return DataType.text();
        }
    }

    /**
     * return the DataType based on a map that contains the collection type as the key and the item type as value. Default value is list of DataType.text();
     */
    public static DataType resolveDataTypeFromMap(Map<String, Object> dataType) {

        DataType.Name collection = null;
        String name = null;
        String key = null;
        String value = null;

        for (Map.Entry<String, Object> entry : dataType.entrySet()) {
            collection = DataType.Name.valueOf(entry.getKey().toUpperCase());
            if (entry.getValue() instanceof Map) {
                key = ((Map) entry.getValue()).keySet().iterator().next().toString();
                value = ((Map) entry.getValue()).values().iterator().next().toString();
            } else {
                name = String.valueOf(entry.getValue());
            }
        }

        switch (collection) {
            case LIST:
                return DataType.list(resolveDataTypeFromString(name));
            case SET:
                return DataType.set(resolveDataTypeFromString(name));
            case MAP:
                return DataType.map(resolveDataTypeFromString(key), resolveDataTypeFromString(value));
            default:
                return DataType.list(DataType.text());
        }
    }

    /**
     * retrieves the list of columns that will construct the partition key
     */
    private static List<ColumnInput> getPartitionKey(List<ColumnInput> columns) {
        List partitionKey = new ArrayList();
        for (ColumnInput column : columns) {
            if (column.isPrimaryKey()) {
                partitionKey.add(column);
            }
        }
        return partitionKey;
    }

    /**
     * retrieves the list of columns that are not primary key
     */
    private static List<ColumnInput> getColumnsThatAreNotPrimaryKey(List<ColumnInput> columns) {
        List columnsList = new ArrayList();
        for (ColumnInput column : columns) {
            if (!column.isPrimaryKey()) {
                columnsList.add(column);
            }
        }
        return columnsList;
    }
}
