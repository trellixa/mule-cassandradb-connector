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

import com.mulesoft.mule.cassandradb.api.IndexExpresion;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TypeInferringSerializer;
import me.prettyprint.hector.api.Serializer;
import org.apache.cassandra.thrift.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Utility class to make the transformations from String and collections to the types that the API handles
 */
public class CassandraDBUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraDBUtils.class);

    private CassandraDBUtils() {
        // Utility class not meant to be instantiated.
    }

    /**
     * Utility method that retrieves a Cassandra Client.
     *
     * @param host     Cassandra DB host to connect.
     * @param port     Cassandra DB port to connect.
     * @param keyspace Default keyspace to be used for rest of the operations.
     * @param username A username.
     * @param password A password.
     * @param tr       A thrift transport.
     * @return A CassandraDB client.
     * @throws org.mule.api.ConnectionException
     */
    public static Cassandra.Client getClient(String host, int port, String keyspace,
                                             String username, String password, TTransport tr) throws org.mule.api.ConnectionException {
        Cassandra.Client client;
        try {
            LOGGER.debug("Attempting to connect to Cassandra");
            tr = new TFramedTransport(new TSocket(host, port)); // NOSONAR
            TProtocol proto = new TBinaryProtocol(tr);
            client = new Cassandra.Client(proto);
            tr.open();
            client.set_keyspace(keyspace);


            if (password != null && username != null) {
                Map<String, String> credentials = new HashMap<String, String>();
                credentials.put("user", username);
                credentials.put("password", password);
                AuthenticationRequest loginRequest = new AuthenticationRequest(
                        credentials);
                client.login(loginRequest);
            }

            LOGGER.debug("Connection created: " + tr);
        } catch (AuthenticationException authEx) {
            LOGGER.error("Invalid user name and password", authEx);
            throw new org.mule.api.ConnectionException(
                    org.mule.api.ConnectionExceptionCode.INCORRECT_CREDENTIALS,
                    null, authEx.getMessage(), authEx);
        } catch (Exception e) {
            LOGGER.error("Unable to connect to Casssandra DB instance", e);
            throw new org.mule.api.ConnectionException(
                    org.mule.api.ConnectionExceptionCode.UNKNOWN, null,
                    e.getMessage(), e);
        }

        return client;
    }

    /**
     * Utility method that converts a Cassandra Column to a JSON Node.
     *
     * @param column Basic unit of data within a ColumnFamily.
     * @return A JSON Node representation of a Cassandra Column.
     * @throws CassandraDBException Generic Exception wrapper class for Thrift Exceptions.
     */
    public static JsonNode columnToJSONNode(Column column) throws CassandraDBException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = mapper.createObjectNode();
            node.put(new String(column.getName(), Charset.defaultCharset()), new String(column.getValue(), Charset.defaultCharset()));

            return node;
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     * Utility method that populates a Map from a Column.
     *
     * @param columnMap     final Map representation of Column after transformation.
     * @param column        Column to be populated as Map
     * @param serializerMap Type of Map serializer to used for Column transformation.
     */
    @SuppressWarnings({"unchecked"})
    public static void populateMap(Map columnMap, Column column, Map<String, Serializer> serializerMap) {
        String name = new String(column.getName(), Charset.defaultCharset());
        Object value;
        if (serializerMap.containsKey(name)) {
            value = serializerMap.get(name).fromBytes(column.getValue());
        } else {
            value = new StringSerializer().fromBytes(column.getValue());
        }

        columnMap.put(name, value);
    }

    /**
     * Retrieves a Map of Serializers.
     *
     * @param columnSerializers Pair of values used to serialize columns
     * @return A Map of Serializers.
     * @throws CassandraDBException Generic Exception wrapper class for Thrift Exceptions.
     */
    static Map<String, Serializer> getSerializationMap(List<ColumnSerializer> columnSerializers) throws CassandraDBException {

        Map<String, Serializer> result = new HashMap<String, Serializer>();

        for (ColumnSerializer columnSerializer : columnSerializers) {
            Serializer serializer;
            try {
                serializer = SerializerTypeInferer.getSerializer(Class.forName(columnSerializer.getType()));
            } catch (ClassNotFoundException e) {
                throw new CassandraDBException(e);
            }

            if (serializer == null) {
                serializer = new ObjectSerializer();
            }

            result.put(columnSerializer.getKey(), serializer);
        }

        return result;
    }

    /**
     * Utility method that converts a Column or SuperColumn to a Map representation.
     *
     * @param columnOrSuperColumn Column or SuperColumn to be represented as a Map.
     * @param columnSerializers   List of ColumnSerializers to be used.
     * @return A Map representation of Column or SuperColumn.
     * @throws CassandraDBException Generic Exception wrapper class for Thrift Exceptions.
     */
    @SuppressWarnings({"unchecked"})
    public static Map columnOrSuperColumnToMap(ColumnOrSuperColumn columnOrSuperColumn,
                                               List<ColumnSerializer> columnSerializers) throws CassandraDBException {
        Map<String, Serializer> serializerMap;

        if (columnSerializers != null) {
            serializerMap = CassandraDBUtils.getSerializationMap(columnSerializers);
        } else {
            serializerMap = new HashMap<String, Serializer>();
        }

        if (columnOrSuperColumn.isSetColumn()) {

            Map columnMap = new HashMap();
            populateMap(columnMap, columnOrSuperColumn.getColumn(), serializerMap);
            return columnMap;
        } else if (columnOrSuperColumn.isSetSuper_column()) {
            SuperColumn superColumn = columnOrSuperColumn.getSuper_column();
            String superColumnName = new String(superColumn.getName(), Charset.defaultCharset());

            Map superColumnNode = new HashMap();

            Map columnsNode = new HashMap();

            List<Column> columns = superColumn.columns;
            for (Column nextColumn : columns) {
                populateMap(columnsNode, nextColumn, serializerMap);
            }
            superColumnNode.put(superColumnName, columnsNode);

            return superColumnNode;
        } else if (columnOrSuperColumn.isSetCounter_column()) {
            Map counterColumnMap = new HashMap();
            CounterColumn counterColumn = columnOrSuperColumn.getCounter_column();
            counterColumnMap.put(new String(counterColumn.getName(), Charset.defaultCharset()), counterColumn.getValue());
            return counterColumnMap;
        } else {
            Map counterSuperColumnMap = new HashMap();
            CounterSuperColumn counterSuperColumn = columnOrSuperColumn.getCounter_super_column();
            String counterSuperColumnName = new String(counterSuperColumn.getName(), Charset.defaultCharset());

            Map columnsNode = new HashMap();

            List<CounterColumn> counterColumns = counterSuperColumn.getColumns();

            for (CounterColumn nextColumn : counterColumns) {
                columnsNode.put(new String(nextColumn.getName(), Charset.defaultCharset()), nextColumn.getValue());
            }
            counterSuperColumnMap.put(counterSuperColumnName, columnsNode);

            return counterSuperColumnMap;
        }
    }

    /**
     * Utility method that converts a Column or SuperColumn to a JSON Node representation.
     *
     * @param columnOrSuperColumn Column or SuperColumn to be represented as a JSON Node.
     * @return A JSONNode representation of Column or SuperColumn.
     * @throws CassandraDBException Generic Exception wrapper class for Thrift Exceptions.
     */
    public static JsonNode columnOrSuperColumnToJSONNode(ColumnOrSuperColumn columnOrSuperColumn) throws CassandraDBException {

        if (columnOrSuperColumn.isSetColumn()) {
            return CassandraDBUtils.columnToJSONNode(columnOrSuperColumn.getColumn());
        } else {
            try {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode superColumnNode = mapper.createObjectNode();
                SuperColumn superColumn = columnOrSuperColumn.getSuper_column();
                String superColumnName = new String(superColumn.getName(), Charset.defaultCharset());
                ObjectNode columnsNode = mapper.createObjectNode();

                List<Column> columns = superColumn.columns;
                for (Column nextColumn : columns) {
                    String name = new String(nextColumn.getName(), Charset.defaultCharset());
                    String value = new String(nextColumn.getValue(), Charset.defaultCharset());
                    columnsNode.put(name, value);
                }
                superColumnNode.put(superColumnName, columnsNode);

                return superColumnNode;
            } catch (Exception e) {
                throw new CassandraDBException(e.getMessage(), e);
            }
        }
    }

    /**
     * Utility method that transforms a List<ColumnOrSuperColumn> to a JSON Node.
     *
     * @param listOfColumns List of Column or SuperColumn to be transformed.
     * @return A JSONNode representation of a List of Column or SuperColumn.
     * @throws CassandraDBException Generic Exception wrapper class for Thrift Exceptions.
     */
    public static JsonNode listOfColumnsToJSONNode(List<ColumnOrSuperColumn> listOfColumns) throws CassandraDBException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ArrayNode arrayNode = mapper.createArrayNode();
            for (ColumnOrSuperColumn nextCol : listOfColumns) {
                arrayNode.add(columnOrSuperColumnToJSONNode(nextCol));
            }

            return arrayNode;
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     * Utility method that transforms a List<ColumnOrSuperColumn> to a List of Maps of Column or SuperColumn.
     *
     * @param listOfColumns     List of Column or SuperColumn to be transformed.
     * @param columnSerializers List of ColumnSerializers to be used.
     * @return A List representation of a Map of Column or SuperColumn.
     * @throws CassandraDBException Generic Exception wrapper class for Thrift Exceptions.
     */
    @SuppressWarnings({"unchecked"})
    public static List listOfColumnsToMap(List<ColumnOrSuperColumn> listOfColumns,
                                          List<ColumnSerializer> columnSerializers) throws CassandraDBException {
        List results = new ArrayList();
        for (ColumnOrSuperColumn nextColumn : listOfColumns) {
            results.add(columnOrSuperColumnToMap(nextColumn, columnSerializers));
        }
        return results;
    }

    /**
     * Utility method that transforms a JSON Node to a String
     *
     * @param node A JSON Node to be transformed.
     * @return A String representation of a JSON Node.
     * @throws CassandraDBException Generic Exception wrapper class for Thrift Exceptions.
     */
    public static String jsonNodeToString(JsonNode node) throws CassandraDBException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(node);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     * Utility method that transforms an Object to a ByteBuffer.
     *
     * @param value An Object type to be transformed.
     * @return A ByteBuffer form of value Object.
     */
    @SuppressWarnings({"unchecked"})
    public static ByteBuffer toByteBuffer(Object value) {

        if (typeInferringSerializer == null) {
            typeInferringSerializer = new TypeInferringSerializer();
        }
        return typeInferringSerializer.toByteBuffer(value);
    }

    /**
     * Utility method to parse a String into a ColumnPath Object.
     *
     * @param columnPath Column Path to be transformed.
     * @return a ColumnPath from a String path.
     */
    public static ColumnPath parseColumnPath(String columnPath) {
        String[] pathElements = columnPath.split(":");

        ColumnPath cPath = new ColumnPath();
        if (pathElements.length > 0) {
            cPath = cPath.setColumn_family(pathElements[0]);
        }
        if (pathElements.length > 1 && pathElements[1].length() > 0) {
            cPath = cPath.setSuper_column(CassandraDBUtils.toByteBuffer(pathElements[1]));
        }
        if (pathElements.length > 2) {
            cPath = cPath.setColumn(CassandraDBUtils.toByteBuffer(pathElements[2]));
        }
        return cPath;
    }

    /**
     * Utility method to generate a ColumnParent object from String.
     *
     * @param columnParent Column Parent to be transformed.
     * @return a ColumnParent Object from a String column parent.
     * @throws CassandraDBException
     */
    public static ColumnParent generateColumnParent(String columnParent) throws CassandraDBException {
        String[] pathElements = columnParent.split(":");

        ColumnParent cParent = new ColumnParent();
        if (pathElements.length > 0) {
            cParent = cParent.setColumn_family(pathElements[0]);
        }
        if (pathElements.length > 1) {
            cParent = cParent.setSuper_column(CassandraDBUtils.toByteBuffer(pathElements[1]));
        }
        return cParent;
    }

    /**
     * Utility method to generate a slice range.
     *
     * @param start    The column name to start the slice with
     * @param finish   The column name to stop the slice at.
     * @param reversed Whether the results should be ordered in reversed order.
     * @param count    How many columns to return.
     * @return a slice range.
     */
    public static SliceRange generateSliceRange(String start, String finish,
                                                boolean reversed, int count) {
        SliceRange range = new SliceRange();
        if (start == null) {
            range.setStart(new byte[0]);
        } else {
            range.setStart(CassandraDBUtils.toByteBuffer(start));
        }

        if (finish == null) {
            range.setFinish(new byte[0]);
        } else {
            range.setFinish(CassandraDBUtils.toByteBuffer(finish));
        }
        range.setCount(count);
        range.setReversed(reversed);
        return range;
    }

    private static TypeInferringSerializer typeInferringSerializer;

    /**
     * Utility method to transforms a list of rowKeys to a ByteBuffer List.
     *
     * @param rowKeys List of rowKeys to be transformed.
     * @return List of rowKeys of type ByteBuffer.
     */
    public static List<ByteBuffer> toByteBufferList(List<String> rowKeys) {

        List<ByteBuffer> list = new ArrayList<ByteBuffer>();

        for (String key : rowKeys) {
            list.add(toByteBuffer(key));
        }

        return list;
    }

    /**
     * Utility method to convert a List of IndexExpression of type String to List of IndexExpression of type ByteBuffer.
     *
     * @param list List of IndexExpression.
     * @return ByteBuffer type of IndexExpression List.
     */
    @SuppressWarnings("unchecked")
    public static List<IndexExpression> toIndexExpression(List<IndexExpresion> list) {
        return (List<IndexExpression>) CollectionUtils.collect((List) list, new Transformer() {
            @Override
            public Object transform(Object input) {
                IndexExpresion sExp = (IndexExpresion) input;
                IndexExpression exp = new IndexExpression();
                exp.setColumn_name(CassandraDBUtils.toByteBuffer(sExp.getColumnName()));
                exp.setOp(sExp.getOp());
                exp.setValue(CassandraDBUtils.toByteBuffer(sExp.getValue()));
                return exp;
            }
        });
    }

    /**
     * Utility method to convert a list of String into List of ColumnDefinition.
     *
     * @param list     A collection of strings to be transformed.
     * @param keyspace Cassandra keyspace to be set.
     * @return the result as a list of CfDef.
     */
    @SuppressWarnings("unchecked")
    public static List<CfDef> toColumnDefinition(List<String> list, final String keyspace) {
        return (List<CfDef>) CollectionUtils.collect((List) list, new Transformer() {
            @Override
            public Object transform(Object input) {
                String name = (String) input;
                CfDef cfDef = new CfDef();
                cfDef.setKeyspace(keyspace);
                cfDef.setName(name);
                return cfDef;
            }
        });
    }

    /**
     * Utility method to retrieve a ColumnDef list from Column Metadata Map.
     *
     * @param columnMetadata a map of column metadata.
     * @return the result as a List of ColumnDef.
     */
    public static List<ColumnDef> toColumnDefinition(Map<String, String> columnMetadata) {
        List<ColumnDef> list = new ArrayList<ColumnDef>();
        Iterator it = columnMetadata.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry) it.next();
            String key = pairs.getKey().toString();
            String validationClass = pairs.getValue().toString();
            ColumnDef cd = new ColumnDef();
            cd.setName(key.getBytes(Charset.defaultCharset()));
            cd.setValidation_class(validationClass);
            list.add(cd);
        }

        return list;
    }
}