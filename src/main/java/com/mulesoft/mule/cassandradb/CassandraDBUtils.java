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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 *  Utility class to make the transformations from String and collections to the types that the API handles
 */
public class CassandraDBUtils {

    protected static final Log logger = LogFactory.getLog(CassandraDBUtils.class);

    public static Cassandra.Client getClient(String host, int port, String keyspace,
                                      String username, String password,  TTransport tr) throws org.mule.api.ConnectionException{
        Cassandra.Client client;
        try {
            logger.debug("Attempting to connect to Cassandra");
            tr = new TFramedTransport(new TSocket(host, port));
            TProtocol proto = new TBinaryProtocol(tr);
            client = new Cassandra.Client(proto);
            tr.open();
            client.set_keyspace(keyspace);


            if (password != null && username != null) {
                HashMap<String, String> credentials = new HashMap<String, String>();
                credentials.put("user", username);
                credentials.put("password", password);
                AuthenticationRequest loginRequest = new AuthenticationRequest(
                        credentials);
                client.login(loginRequest);
            }

            logger.debug("Connection created: " + tr);
        } catch (AuthenticationException authEx) {
            logger.error("Invalid user name and password", authEx);
            throw new org.mule.api.ConnectionException(
                    org.mule.api.ConnectionExceptionCode.INCORRECT_CREDENTIALS,
                    null, authEx.getMessage(), authEx);
        } catch (Throwable e) {
            logger.error("Unable to connect to Casssandra DB instance", e);
            throw new org.mule.api.ConnectionException(
                    org.mule.api.ConnectionExceptionCode.UNKNOWN, null,
                    e.getMessage(), e);
        }

        return client;
    }

    public static JsonNode columnToJSONNode(Column column) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        String name = new String(column.getName());
        String value = new String(column.getValue());
        node.put(name, value);
        return node;
    }

    @SuppressWarnings({"unchecked"})
    public static void populateMap(Map columnMap,Column column, Map<String, Serializer> serializerMap)  {
        String name = new String(column.getName());
        Object value;
        if (serializerMap.containsKey(name)) {
            value = serializerMap.get(name).fromBytes(column.getValue());
        } else {
            value = new StringSerializer().fromBytes(column.getValue());
        }
        
        columnMap.put(name, value);
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
                                               List<ColumnSerializer> columnSerializers) throws UnsupportedEncodingException {


        Map<String, Serializer> serializerMap;

        if (columnSerializers != null) {
            serializerMap = CassandraDBUtils.getSerializationMap(columnSerializers);
        } else {
            serializerMap = new HashMap<String, Serializer>();
        }

        if (columnOrSuperColumn.isSetColumn()) {
        	
        	Map columnMap = new HashMap();
        	populateMap(columnMap,columnOrSuperColumn.getColumn(),serializerMap);
        	return columnMap;
        } else if (columnOrSuperColumn.isSetSuper_column()) {
            SuperColumn superColumn = columnOrSuperColumn.getSuper_column();
            String superColumnName = new String(superColumn.getName());

            Map superColumnNode = new HashMap();

            Map columnsNode = new HashMap();

            List<Column> columns = superColumn.columns;
            for (Column nextColumn : columns) {
            	populateMap(columnsNode,nextColumn,serializerMap);
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
                                          List<ColumnSerializer> columnSerializers) throws UnsupportedEncodingException {
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

    	if(typeInferringSerializer==null){
    		typeInferringSerializer = new TypeInferringSerializer();
    	}
        return typeInferringSerializer.toByteBuffer(value);
    }

    public static ColumnPath parseColumnPath(String columnPath) throws java.io.UnsupportedEncodingException {
        String[] pathElements = columnPath.split(":");

        ColumnPath cPath = new ColumnPath();
        if (pathElements.length > 0)
            cPath = cPath.setColumn_family(pathElements[0]);
        if (pathElements.length > 1){
        	if(pathElements[1].length()>0)
                cPath = cPath.setSuper_column(CassandraDBUtils.toByteBuffer(pathElements[1]));
        }
        if (pathElements.length > 2){
            cPath = cPath.setColumn(CassandraDBUtils.toByteBuffer(pathElements[2]));
        }
        return cPath;
    }
    
	public static ColumnParent generateColumnParent(String columnParent)
			throws UnsupportedEncodingException {
		String[] pathElements = columnParent.split(":");

        ColumnParent cParent = new ColumnParent();
        if (pathElements.length > 0)
            cParent = cParent.setColumn_family(pathElements[0]);
        if (pathElements.length > 1)
            cParent = cParent.setSuper_column(CassandraDBUtils.toByteBuffer(pathElements[1]));
		return cParent;
	}
	
	public static SliceRange generateSliceRange(String start, String finish,
			boolean reversed, int count) throws UnsupportedEncodingException {
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

	public static List<ByteBuffer> toByteBufferList(List<String> rowKeys) throws UnsupportedEncodingException {
		
		List<ByteBuffer> list = new ArrayList<ByteBuffer>();
		
		for(String key : rowKeys){
			list.add(toByteBuffer(key));
		}
		
		return list;
	}
	
    @SuppressWarnings("unchecked")
    public static List<IndexExpression> toIndexExpression(List<IndexExpresion> list)
    {
        return (List<IndexExpression>) CollectionUtils.collect((List)list, new Transformer()
        {
            @Override
            public Object transform(Object input)
            {
            	IndexExpresion sExp= (IndexExpresion)input;
            	IndexExpression exp = new IndexExpression();
            	try {
					exp.setColumn_name(CassandraDBUtils.toByteBuffer(sExp.getColumnName()));
					exp.setOp(sExp.getOp());
					exp.setValue(CassandraDBUtils.toByteBuffer(sExp.getValue()));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}

                return exp;
            }
        });
    }
    @SuppressWarnings("unchecked")
    public static List<CfDef> toColumnDefinition(List<String> list,final String keyspace)
    {
        return (List<CfDef>) CollectionUtils.collect((List)list, new Transformer()
        {
            @Override
            public Object transform(Object input)
            {
            	String name= (String)input;
            	CfDef cfDef = new CfDef();
            	cfDef.setKeyspace(keyspace);
            	cfDef.setName(name);
                return cfDef;
            }
        });
    }

	public static List<ColumnDef> toColumnDefinition(
			Map<String, String> columnMetadata) {
		List<ColumnDef> list = new ArrayList<ColumnDef>();
		Iterator it = columnMetadata.entrySet().iterator();
	    
		while (it.hasNext()) {
	        Map.Entry pairs = (Map.Entry)it.next();
	        String key = pairs.getKey().toString();
	        String validationClass = pairs.getValue().toString();
	        ColumnDef cd=new ColumnDef();
	        cd.setName(key.getBytes());
	        cd.setValidation_class(validationClass);
	        list.add(cd);
	    }
		
	    return list;
	}
}