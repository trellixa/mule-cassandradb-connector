package com.mulesoft.mule.cassandradb;

import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import org.apache.cassandra.thrift.*;

import java.nio.ByteBuffer;

import java.util.*;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.JsonNode;

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

            if (columnSerializer.getType().equals("string")) {
                serializer = new StringSerializer();
            }

            if (columnSerializer.getType().equals("double")) {
                serializer = new DoubleSerializer();
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
        Iterator<ColumnOrSuperColumn> cols = listOfColumns.iterator();
        while (cols.hasNext()) {
            ColumnOrSuperColumn nextCol = cols.next();
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

    public static ByteBuffer toByteBuffer(Object value) throws UnsupportedEncodingException {
        if (value instanceof String) {
            StringSerializer serializer = new StringSerializer();
            return serializer.toByteBuffer((String) value);
        } else if (value instanceof Double) {
            DoubleSerializer serializer = new DoubleSerializer();
            return serializer.toByteBuffer((Double) value);
        } else {
            ObjectSerializer serializer = new ObjectSerializer();
            return serializer.toByteBuffer(value);
        }
    }
}