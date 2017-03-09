/**
 * Mule Cassandra Connector
 * <p>
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * <p>
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.mulesoft.mule.cassandradb;

import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mule.api.ConnectionException;

import static org.mockito.Mockito.when;

public class CassandraDBConnectorMockTest {

    private static CassandraDBConnector connector = new CassandraDBConnector();
    private static CassandraClient client = new CassandraClient();

    @BeforeClass
    public static void setUpTests() throws ConnectionException {
        connector.setHost("127.0.0.1");
        connector.setPort(9042);
        connector.setClient(client);
        connector.connect(null, null);
    }

    @AfterClass
    public static void tearDown() {

        client.close();
    }

    @Test
    public void shouldCreateKeyspace() throws CassandraDBException {
        when(connector.createKeyspace("dummy_keyspace", null)).thenReturn(Mockito.any());
    }

    @Test
    public void shouldDropKeyspace() throws CassandraDBException {
//        when(connector.dropKeyspace("dummy_keyspace"));
        connector.dropKeyspace("dummy_keyspace");
//        asse
    }

    @Test
    public void shouldCreateTable() {
        try {
            connector.createTable("dummy table", null);
        } catch (CassandraDBException e) {
            e.printStackTrace();
        }
    }
//
//    @Test
//    public void testDescribeSchema() throws Exception {
//        when(client.describe_cluster_name()).thenReturn(fakeDescription);
//        assertEquals(fakeDescription, connector.describeClusterName());
//    }
//
//    @Test
//    public void testAddKeyspace() throws Exception {
//        when(client.system_add_keyspace(keyspaceDefinition)).thenReturn(schemaVersionID1);
//        ArrayList<String> columnNames = new ArrayList<String>();
//        columnNames.add(column1);
//        assertEquals(schemaVersionID1, connector.systemAddKeyspaceWithParams(keyspace, columnNames, null, null));
//    }
//
//    @Test
//    public void testMultiGetCount() throws Exception {
//        Map<ByteBuffer, Integer> map = new HashMap<ByteBuffer, Integer>();
//        List<String> rowKeys = new ArrayList<String>();
//        rowKeys.add(rowKey);
//        List<ByteBuffer> keys = CassandraDBUtils.toByteBufferList(rowKeys);
//        when(client.multiget_count(keys, columnParent, predicate, consistencyLevel)).thenReturn(map);
//        assertEquals(map, connector.multiGetCount(rowKeys, columnFamily, null, null, false, 50));
//    }
//
//    @Test
//    public void testGet() throws Exception {
//        ColumnOrSuperColumn result = new ColumnOrSuperColumn();
//        Column column = new Column();
//        column.setName("foo".getBytes());
//        column.setValue(SerializationUtils.serialize("foo"));
//        result.setColumn(column);
//        when(client.get(any(ByteBuffer.class), any(cPath.getClass()), any(ConsistencyLevel.class))).
//                thenReturn(result);
//        assertNotNull(connector.get("foo", "foo", null));
//    }
//
//    @Test
//    public void testGetRow() throws Exception {
//        ColumnOrSuperColumn result = new ColumnOrSuperColumn();
//        Column column = new Column();
//        column.setName("foo".getBytes());
//        column.setValue(SerializationUtils.serialize("foo"));
//        result.setColumn(column);
//        when(client.get(any(ByteBuffer.class), any(cPath.getClass()), any(ConsistencyLevel.class))).
//                thenReturn(result);
//        ColumnPath path = new ColumnPath();
//        assertNotNull(connector.getRow("foo", path, null));
//    }
//
//    @Test
//    public void testGetSlice() throws Exception {
//        List<KeySlice> result = new ArrayList<KeySlice>();
//        KeySlice slice = new KeySlice();
//        slice.setKey(SerializationUtils.serialize("foo"));
//        List<ColumnOrSuperColumn> columns = new ArrayList<ColumnOrSuperColumn>();
//        ColumnOrSuperColumn c = new ColumnOrSuperColumn();
//        Column column = new Column();
//        column.setName("foo".getBytes());
//        column.setValue("foo".getBytes());
//        c.setColumn(column);
//        columns.add(c);
//        slice.setColumns(columns);
//        result.add(slice);
//        when(client.get_slice(any(ByteBuffer.class), any(ColumnParent.class)
//                , any(SlicePredicate.class), any(ConsistencyLevel.class))).thenReturn(columns);
//        assertNotNull(connector.getSlice("foo", "foo", null, null, false, 1, null));
//    }
//
//    @Test
//    public void testGetCount() throws Exception {
//        when(client.get_count(any(ByteBuffer.class), any(ColumnParent.class), any(SlicePredicate.class),
//                any(ConsistencyLevel.class)))
//                .thenReturn(1);
//        assertEquals(1, connector.getCount("foo", "foo", null, null, false, 1));
//    }
//
//    @Test
//    public void testGetRangeSlices() throws Exception {
//        List<KeySlice> result = new ArrayList<KeySlice>();
//        KeySlice slice = new KeySlice();
//        slice.setKey(SerializationUtils.serialize("foo"));
//        List<ColumnOrSuperColumn> columns = new ArrayList<ColumnOrSuperColumn>();
//        columns.add(new ColumnOrSuperColumn());
//        slice.setColumns(columns);
//        result.add(slice);
//        when(client.get_range_slices(any(ColumnParent.class), any(SlicePredicate.class),
//                any(KeyRange.class), any(ConsistencyLevel.class))).thenReturn(result);
//        connector.getRangeSlices("foo", null, null, false, 1, null, null, null, null, 1);
//    }
//
//    @Test
//    public void testGetIndexedSlices() throws Exception {
//        List<KeySlice> result = new ArrayList<KeySlice>();
//        KeySlice slice = new KeySlice();
//        slice.setKey(SerializationUtils.serialize("foo"));
//        List<ColumnOrSuperColumn> columns = new ArrayList<ColumnOrSuperColumn>();
//        columns.add(new ColumnOrSuperColumn());
//        slice.setColumns(columns);
//        result.add(slice);
//        when(client.get_range_slices(any(ColumnParent.class), any(SlicePredicate.class),
//                any(KeyRange.class), any(ConsistencyLevel.class))).thenReturn(result);
//        com.mulesoft.mule.cassandradb.api.IndexExpresion expresion = new
//                com.mulesoft.mule.cassandradb.api.IndexExpresion();
//        connector.getIndexedSlices("foo", null, null, false, 1, 1, null, Collections.singletonList(expresion));
//    }
//
//    @Test
//    public void testInsert() throws Exception {
//        Mockito.doNothing().when(client)
//                .insert(any(ByteBuffer.class), any(ColumnParent.class), any(Column.class), any(ConsistencyLevel.class));
//        connector.insert("foo", "foo", "foo", "foo", 0);
//    }
//
//    @Test
//    public void testInsertFromMap() throws Exception {
//        Mockito.doNothing().when(client)
//                .insert(any(ByteBuffer.class), any(ColumnParent.class), any(Column.class), any(ConsistencyLevel.class));
//        Map contentMap = new HashMap();
//        Map usersMap = new HashMap();
//        Map superColumn = new HashMap();
//        Map columns = new HashMap();
//        columns.put("foo", "foo");
//        superColumn.put("foo", columns);
//        usersMap.put("foo", superColumn);
//        contentMap.put("foo", usersMap);
//        connector.insertFromMap(contentMap);
//    }
//
//    @Test
//    public void testBatchMutable() throws Exception {
//        Mockito.doNothing().when(client).batch_mutate(any(Map.class), any(ConsistencyLevel.class));
//        connector.batchMutable(new HashMap());
//    }
//
//    @Test
//    public void testAdd() throws Exception {
//        Mockito.doNothing().when(client)
//                .add(any(ByteBuffer.class), any(ColumnParent.class), any(CounterColumn.class), any(ConsistencyLevel.class));
//        connector.add("foo", "foo", "foo", 1);
//    }
//
//    @Test
//    public void testRemove() throws Exception {
//        Mockito.doNothing().when(client).remove(any(ByteBuffer.class), any(ColumnPath.class), anyLong(),
//                any(ConsistencyLevel.class));
//        connector.remove("foo", "foo");
//    }
//
//    @Test
//    public void testRemoveCounter() throws Exception {
//        Mockito.doNothing().when(client).remove_counter(any(ByteBuffer.class), any(ColumnPath.class),
//                any(ConsistencyLevel.class));
//        connector.removeCounter("foo", "foo");
//    }
//
//    @Test
//    public void testTruncate() throws Exception {
//        Mockito.doNothing().when(client).truncate(anyString());
//        connector.truncate("foo");
//    }
//
//    @Test
//    public void testDescribeClusterName() throws Exception {
//        when(client.describe_cluster_name()).thenReturn("foo");
//        assertEquals("foo", connector.describeClusterName());
//    }
//
//    @Test
//    public void testDescribeSchemaVersions() throws Exception {
//        when(client.describe_schema_versions()).thenReturn(new HashMap<String, List<String>>());
//        Assert.assertNotNull(connector.describeSchemaVersions());
//    }
//
//    @Test
//    public void testDescribeKeyspace() throws Exception {
//        when(client.describe_keyspace(anyString())).thenReturn(new KsDef());
//        Assert.assertNotNull(connector.describeKeyspace("foo"));
//    }
//
//    @Test
//    public void testDescribeKeyspaces() throws Exception {
//        when(client.describe_keyspaces()).thenReturn(new ArrayList<KsDef>());
//        Assert.assertNotNull(connector.describeKeyspaces());
//    }
//
//    @Test
//    public void testDescribePartitioner() throws Exception {
//        when(client.describe_partitioner()).thenReturn("foo");
//        assertEquals("foo", connector.describePartitioner());
//    }
//
//    @Test
//    public void testDescribeRing() throws Exception {
//        when(client.describe_ring(anyString())).thenReturn(new ArrayList<TokenRange>());
//        Assert.assertNotNull(connector.describeRing("foo"));
//    }
//
//    @Test
//    public void testDescribeSnitch() throws Exception {
//        when(client.describe_snitch()).thenReturn("foo");
//        assertEquals("foo", connector.describeSnitch());
//    }
//
//    @Test
//    public void testDescribeVersion() throws Exception {
//        when(client.describe_version()).thenReturn("1.0");
//        assertEquals("1.0", connector.describeVersion());
//    }
//
//    @Test
//    public void testSystemAddColumnFamilyFromObject() throws Exception {
//        when(client.system_add_column_family(any(CfDef.class))).thenReturn("foo");
//        assertEquals("foo", connector.systemAddColumnFamilyFromObject(new CfDef()));
//    }
//
//    @Test
//    public void testSystemAddColumnFamilyFromObjectWithSimpleNames() throws Exception {
//        CfDef cfDef = new CfDef();
//        ColumnDef metadata = new ColumnDef();
//        metadata.setName("foo".getBytes());
//        cfDef.setColumn_metadata(Collections.singletonList(metadata));
//        List<String> names = new ArrayList<String>();
//        names.add("foo");
//        connector.systemAddColumnFamilyFromObjectWithSimpleNames(cfDef, names);
//    }
//
//    @Test
//    public void testSystemAddColumnFamilyWithParams() throws Exception {
//        connector.systemAddColumnFamilyWithParams("foo", null, null, null, null, null);
//    }
//
//    @Test
//    public void testSystemDropColumnFamily() throws Exception {
//        when(client.system_drop_column_family(anyString())).thenReturn("foo");
//        assertEquals("foo", connector.systemDropColumnFamily("foo"));
//    }
//
//    @Test
//    public void testSystemAddKeyspaceFromObject() throws Exception {
//        when(client.system_add_keyspace(any(KsDef.class))).thenReturn("foo");
//        KsDef ksDef = new KsDef();
//        connector.systemAddKeyspaceFromObject(ksDef);
//    }
//
//    @Test
//    public void testSystemAddKeyspaceWithParams() throws Exception {
//        connector.systemAddColumnFamilyWithParams("foo", null, null, null, null, null);
//    }
//
//    @Test
//    public void testSytemDropKeyspace() throws Exception {
//        connector.systemDropKeyspace("foo");
//    }
//
//    @Test
//    public void testSytemUpdateKeyspace() throws Exception {
//        connector.systemUpdateKeyspace(new KsDef());
//    }
//
//    @Test
//    public void testSytemUpdateColumnFamily() throws Exception {
//        connector.systemUpdateColumnFamily(new CfDef());
//    }
//
//    @Test
//    public void testExecuteCQLQuery() throws Exception {
//        connector.executeCqlQuery("foo", null);
//    }


}
