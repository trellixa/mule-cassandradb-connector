package com.mulesoft.connectors.cassandradb.internal.util;

import com.datastax.driver.core.DataType;
import com.mulesoft.connectors.cassandradb.api.ColumnType;

public class DataTypeResolver {
    public static DataType resolve(ColumnType type){
        switch (type){
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
}
