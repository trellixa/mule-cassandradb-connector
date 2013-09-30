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

import org.apache.cassandra.thrift.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mule.util.SerializationUtils;

import java.nio.ByteBuffer;
import java.util.*;

import static junit.framework.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;


public class CassandraDBObjectStoreMockTest {

    private static String keyspace = "MyKeyspace";
    private static String fakeDescription = "fakeDescription";
    private static String schemaVersionID1 = "Schema1";
    private static String superColumn1 = "SuperColumn1";
    private static String column1 = "Column1";
    private static String columnFamily = "ColumnFamily";
    private static String rowKey = "001";
    private static String columnPath1 = columnFamily + ":" + superColumn1 + ":" + column1;

    private ColumnParent columnParent;
    private CassandraDBObjectStore objectStore;
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
        consistencyLevel = ConsistencyLevel.ONE;

        objectStore = new CassandraDBObjectStore();
        objectStore.setClient(client);
        objectStore.setConsistencyLevel(consistencyLevel);
        objectStore.setKeyspace("foo");


        keyspaceDefinition = new KsDef();
        keyspaceDefinition.setName(keyspace);
        keyspaceDefinition.setStrategy_class(ReplicationStrategy.SIMPLE.toString());
        Map<String, String> options = new HashMap<String, String>();
        options.put("replication_factor", "1");
        keyspaceDefinition.setStrategy_options(options);
        CfDef cfDef = new CfDef();
        cfDef.setName(column1);
        cfDef.setKeyspace(keyspace);
        ArrayList<CfDef> lista = new ArrayList<CfDef>();

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
        cPath = new ColumnPath();
        cPath.setColumn_family(columnFamily);
        cPath.setColumn(CassandraDBUtils.toByteBuffer(column1));

        superColumn = new SuperColumn();
        superColumn.setName(CassandraDBUtils.toByteBuffer(superColumn1));

        cPath.setSuper_column(CassandraDBUtils.toByteBuffer(superColumn1));
        colOrSup = new ColumnOrSuperColumn();
        colOrSup.setSuper_column(superColumn);

        KsDef def = new KsDef();
        def.setName("foo");
        def.setCf_defs(Collections.singletonList(cfDef));
        when(client.describe_keyspace("foo")).thenReturn(def);
    }

    @Test
    public void testContains() throws Exception {
        String payload = "foo";

        when(client.get_count(any(ByteBuffer.class), any(ColumnParent.class), any(SlicePredicate.class),
                any(ConsistencyLevel.class)))
                .thenReturn(1);
        assertTrue(objectStore.contains(payload));
    }

    @Test
    public void testRetrieve() throws Exception {
        ColumnOrSuperColumn result = new ColumnOrSuperColumn();
        Column column = new Column();
        column.setValue(SerializationUtils.serialize("foo"));
        result.setColumn(column);
        when(client.get(any(ByteBuffer.class), any(cPath.getClass()), any(ConsistencyLevel.class))).
                thenReturn(result);
        String key = "foo";
        assertNotNull(objectStore.retrieve(key));
    }

    @Test
    public void testStore() throws Exception {
        Mockito.doNothing().when(client)
                .insert(any(ByteBuffer.class), any(ColumnParent.class), any(Column.class), any(ConsistencyLevel.class));

        boolean exceptionThrown = false;
        try {
            objectStore.store("foo", "foo");
        } catch (Throwable t) {
            exceptionThrown = true;
        }
        assertFalse(exceptionThrown);
    }

    @Test
    @Ignore
    public void testRemove() throws Exception {
        ColumnOrSuperColumn result = new ColumnOrSuperColumn();
        Column column = new Column();
        column.setValue(SerializationUtils.serialize("foo"));
        result.setColumn(column);

        try {
            Mockito.when(client.get(any(ByteBuffer.class), any(ColumnPath.class), any(ConsistencyLevel.class)))
                    .thenReturn(result);
            Mockito.doNothing().when(client)
                    .remove(any(ByteBuffer.class), any(ColumnPath.class), any(Long.class), any(ConsistencyLevel.class));
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertNotNull(objectStore.remove("foo"));
    }

    @Test
    public void testAllKeys() throws Exception {
        List<KeySlice> result = new ArrayList<KeySlice>();
        KeySlice slice = new KeySlice();
        slice.setKey(SerializationUtils.serialize("foo"));
        List<ColumnOrSuperColumn> columns = new ArrayList<ColumnOrSuperColumn>();
        columns.add(new ColumnOrSuperColumn());
        slice.setColumns(columns);
        result.add(slice);
        when(client.get_range_slices(any(ColumnParent.class), any(SlicePredicate.class),
                any(KeyRange.class), any(ConsistencyLevel.class))).thenReturn(result);
        assertNotNull(objectStore.allKeys());
        assertEquals(1, objectStore.allKeys().size());
    }

    @Test
    public void testAllPartitions() throws Exception {
        KsDef def = new KsDef();
        CfDef partition = new CfDef();
        partition.setName("foo");
        def.addToCf_defs(partition);
        when(client.describe_keyspace(any(String.class))).thenReturn(def);
        assertEquals(1, objectStore.allPartitions().size());
    }

    @Test
    public void testDispose() throws Exception {
        Mockito.doNothing().when(client).truncate(any(String.class));
        boolean exceptionThrown = false;

        try {
            objectStore.disposePartition("foo");
        } catch (Throwable t) {
            exceptionThrown = true;
        }
        assertFalse(exceptionThrown);
    }
}

