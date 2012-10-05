/**
 * Mule Cassandra Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.mulesoft.mule.cassandradb;

import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.hector.api.Serializer;
import org.apache.cassandra.thrift.*;

import java.nio.ByteBuffer;

import java.util.*;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.JsonNode;
import org.mule.api.DefaultMuleException;

import java.io.UnsupportedEncodingException;

public class CassandraDBUtils {

    public static JsonNode columnToJSONNode(Column column) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        String name = new String(column.getName());
        String value = new String(column.getValue());
        node.put(name, value);
        return node;
    }

    @SuppressWarnings({"unchecked"})
    public static Map columnToMap(Column column) throws Exception {
        Map result = new HashMap();
        result.put(column.getName(), column.getValue());
        return result;
    }


    static Map<String, Serializer> getSerializationMap(List<ColumnSerializer> columnSerializers) {

        Map<String, Serializer> result = new HashMap<String, Serializer>();

        for (ColumnSerializer columnSerializer : columnSerializers) {
            Serializer serializer = null;
            try {
                serializer = SerializerTypeInferer.getSerializer(Class.forName(columnSerializer.getType()));
            } catch (ClassNotFoundException e) {
                throw new CassandraException(e);
            }

            if (serializer == null) {
                serializer = new ObjectSerializer();
            }

            result.put(columnSerializer.getKey(), serializer);
        }

        return result;
    }


    @SuppressWarnings({"unchecked"})
    public static Map columnOrSuperColumnToMap(ColumnOrSuperColumn columnOrSuperColumn,
                                               List<ColumnSerializer> columnSerializers) throws Exception {

        Map<String, Serializer> serializerMap = CassandraDBUtils.getSerializationMap(columnSerializers);

        if (columnOrSuperColumn.isSetColumn()) {
            return columnToMap(columnOrSuperColumn.getColumn());
        } else {
            SuperColumn superColumn = columnOrSuperColumn.getSuper_column();
            String superColumnName = new String(superColumn.getName());

            Map superColumnNode = new HashMap();

            Map columnsNode = new HashMap();

            List<Column> columns = superColumn.columns;
            for (Column nextColumn : columns) {
                String name = new String(nextColumn.getName());
                Object value = serializerMap.get(name).fromBytes(nextColumn.getValue());
                columnsNode.put(name, value);
            }
            superColumnNode.put(superColumnName, columnsNode);

            return superColumnNode;
        }
    }

    public static JsonNode columnOrSuperColumnToJSONNode(ColumnOrSuperColumn columnOrSuperColumn) throws Exception {

        if (columnOrSuperColumn.isSetColumn()) {
            return CassandraDBUtils.columnToJSONNode(columnOrSuperColumn.getColumn());
        } else {
            SuperColumn superColumn = columnOrSuperColumn.getSuper_column();
            String superColumnName = new String(superColumn.getName());
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode superColumnNode = mapper.createObjectNode();
            ObjectNode columnsNode = mapper.createObjectNode();

            List<Column> columns = superColumn.columns;
            for (Column nextColumn : columns) {
                String name = new String(nextColumn.getName());
                String value = new String(nextColumn.getValue());
                columnsNode.put(name, value);
            }
            superColumnNode.put(superColumnName, columnsNode);

            return superColumnNode;
        }
    }

    public static JsonNode listOfColumnsToJSONNode(List<ColumnOrSuperColumn> listOfColumns) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = mapper.createArrayNode();
        for (ColumnOrSuperColumn nextCol : listOfColumns) {
            arrayNode.add(columnOrSuperColumnToJSONNode(nextCol));
        }
        return arrayNode;
    }

    @SuppressWarnings({"unchecked"})
    public static List listOfColumnsToMap(List<ColumnOrSuperColumn> listOfColumns,
                                          List<ColumnSerializer> columnSerializers) throws Exception {
        List results = new ArrayList();
        for (ColumnOrSuperColumn nextColumn : listOfColumns) {
            results.add(columnOrSuperColumnToMap(nextColumn, columnSerializers));
        }
        return results;
    }

    public static String jsonNodeToString(JsonNode node) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(node);
    }

    @SuppressWarnings({"unchecked"})
    public static ByteBuffer toByteBuffer(Object value) throws UnsupportedEncodingException {
        TypeInferringSerializer typeInferringSerializer = new TypeInferringSerializer();
        return typeInferringSerializer.toByteBuffer(value);
    }
}