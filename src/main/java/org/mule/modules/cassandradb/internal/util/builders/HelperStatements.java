/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.internal.util.builders;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import org.mule.modules.cassandradb.api.*;
import org.mule.modules.cassandradb.internal.exception.CassandraException;

import java.util.ArrayList;
import java.util.List;


public class HelperStatements {
	
	private HelperStatements() {
		// Empty constructor
	}

    public static SchemaStatement createKeyspaceStatement(CreateKeyspaceInput input) {
        //build create keyspace statement if not exists
        CreateKeyspace createKeyspaceStatement = SchemaBuilder.createKeyspace(input.getKeyspaceName()).ifNotExists();

        return createKeyspaceStatement.with().replication(ReplicationStrategy.buildReplicationStrategy(input));
    }

    public static SchemaStatement dropKeyspaceStatement(String keyspaceName) {
        return SchemaBuilder.dropKeyspace(keyspaceName).ifExists();
    }

    public static SchemaStatement createTable(String keyspace, CreateTableInput input) throws CassandraException {

        if (input.getColumns() == null || input.getColumns().isEmpty()) {
            throw new CassandraException("Mismatched input. Cannot create table without columns.");
        }

        Create table = SchemaBuilder.createTable(keyspace, input.getTableName()).ifNotExists();

        List<ColumnInput> partitionKey = getPartitionKey(input.getColumns());
        if (partitionKey.isEmpty()) {
            throw new CassandraException("Mismatched input. Primary key is missing.");
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

    public static SchemaStatement dropTable(String tableName, String keyspaceName) {
        return SchemaBuilder.dropTable(keyspaceName, tableName).ifExists();
    }

    /**
     * return the DataType based on a String. Default value is DataType.text();
     * @param dataType string to be resolved to DataType
     * @return the resolved DataType
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
     * retrieves the list of columns that will construct the partition key
     */
    private static List<ColumnInput> getPartitionKey(List<ColumnInput> columns) {
        List<ColumnInput> partitionKey = new ArrayList<ColumnInput>();
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
        List<ColumnInput> columnsList = new ArrayList<ColumnInput>();
        for (ColumnInput column : columns) {
            if (!column.isPrimaryKey()) {
                columnsList.add(column);
            }
        }
        return columnsList;
    }
}
