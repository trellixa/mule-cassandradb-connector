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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CassandraDBConnectorMockTest {

	private static String keyspace = "MyKeyspace";
	private static String fakeDescription = "fakeDescription";
	private static String schemaVersionID1 = "Schema1";
	private static String superColumn1 = "SuperColumn1";
	private static String column1 = "Column1";
	private static String columnFamily = "ColumnFamily";
	private static String rowKey = "001";
	private static String columnPath1 = columnFamily + ":" + superColumn1 + ":" + column1; 
	
	private ColumnParent columnParent;
	private CassandraDBConnector connector;
	private KsDef keyspaceDefinition;
	private SliceRange range;
	private SlicePredicate predicate;
	private ConsistencyLevel consistencyLevel;
	private ColumnPath cPath;
	private SuperColumn superColumn;
	private ColumnOrSuperColumn colOrSup;
	@Mock 
	private Cassandra.Client client;
    @Before
    public void setUpTests() throws Exception {
        MockitoAnnotations.initMocks(this);
        consistencyLevel= ConsistencyLevel.ONE;
        
        connector = new CassandraDBConnector();
        connector.setClient(client);
        connector.setConsistencyLevel(consistencyLevel);
        
        keyspaceDefinition= new KsDef();
        keyspaceDefinition.setName(keyspace);
        keyspaceDefinition.setStrategy_class(ReplicationStrategy.SIMPLE.toString());
        Map<String,String> options = new HashMap<String,String>();
		options.put("replication_factor", "1");
        keyspaceDefinition.setStrategy_options(options);
		CfDef cfDef=new CfDef();
		cfDef.setName(column1);
		cfDef.setKeyspace(keyspace);
		ArrayList<CfDef> lista= new ArrayList<CfDef>();
		
		lista.add(cfDef);
        keyspaceDefinition.setCf_defs(lista);
        
        range = new SliceRange();
        range.setStart(new byte[0]);
        range.setFinish(new byte[0]);
        range.setCount(50);
        range.setReversed(false);
        
		predicate = new SlicePredicate();
		predicate.setSlice_range(range);
		
        columnParent = new ColumnParent();
        columnParent.setColumn_family(columnFamily);
        cPath=new ColumnPath();
        cPath.setColumn_family(columnFamily);
        cPath.setColumn(CassandraDBUtils.toByteBuffer(column1));
        
        superColumn = new SuperColumn();
        superColumn.setName(CassandraDBUtils.toByteBuffer(superColumn1));

        cPath.setSuper_column(CassandraDBUtils.toByteBuffer(superColumn1));
        colOrSup=new ColumnOrSuperColumn();
        colOrSup.setSuper_column(superColumn);
        
    }

    @Test
    public void testDescribeSchema(){
    	try {
    		when(client.describe_cluster_name()).thenReturn(fakeDescription);
			assertEquals(fakeDescription,connector.describeClusterName());
		} catch (TException e) {
			e.printStackTrace();
		}
    }
    
    @Test
    public void testAddKeyspace(){
    	try {
			when(client.system_add_keyspace(keyspaceDefinition)).thenReturn(schemaVersionID1);
			ArrayList<String> columnNames=new ArrayList<String>();
			columnNames.add(column1);
			assertEquals(schemaVersionID1,connector.systemAddKeyspaceWithParams(keyspace, columnNames, null, null));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }
    
    @Test
    public void testMultiGetCount(){
    	Map<ByteBuffer, Integer> map = new HashMap<ByteBuffer, Integer>();
    	List<String> rowKeys=new ArrayList<String>();
    	rowKeys.add(rowKey);
    	try {
    		List<ByteBuffer> keys = CassandraDBUtils.toByteBufferList(rowKeys);
			when(client.multiget_count(keys, columnParent, predicate, consistencyLevel)).thenReturn(map);
			
			assertEquals(map,connector.multiGetCount(rowKeys, columnFamily, null, null, false, 50));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
