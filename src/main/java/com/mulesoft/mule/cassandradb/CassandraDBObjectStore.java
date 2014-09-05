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
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.mule.RequestContext;
import org.mule.api.MuleContext;
import org.mule.api.MuleEvent;
import org.mule.api.context.MuleContextAware;
import org.mule.api.store.ObjectStoreException;
import org.mule.api.store.PartitionableObjectStore;
import org.mule.util.SerializationUtils;
import org.mule.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Cassandra DB Object Store to manipulate object stores with Mule Context.
 */
public class CassandraDBObjectStore implements PartitionableObjectStore<Serializable>, MuleContextAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraDBObjectStore.class);

    private MuleContext context;

    private String username;

    private String password;

    private String host;

    private String defaultPartitionName;

    private int port = 9160;

    private String keyspace;

    private static String FALLBACK_PARTITION_NAME = "MULE_OBJECT_STORE";

    private static final String OBJECT = "object";

    /**
     * Consistency Level. Can be one of ANY, ONE (default), TWO, THREE, QUORUM,
     * LOCAL_QUORUM, EACH_QUORUM, ALL. See http://wiki.apache.org/cassandra/API
     * for more details.
     */
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    /**
     * Generic class that encapsulates the I/O layer. This is basically a thin
     * wrapper around the combined functionality of Java input/output streams.
     */
    private TTransport tr;
    /**
     * Cassandra Client
     */
    private Cassandra.Client client;

    @Override
    public void setMuleContext(MuleContext muleContext) {
        context = muleContext;
    }

    @Override
    public void store(Serializable key, Serializable value, String partition) throws ObjectStoreException {
        try {
            ColumnParent parent = getColumnFamily(partition);

            Column column = new Column();
            column.setName(CassandraDBUtils.toByteBuffer(OBJECT));
            column.setValue(SerializationUtils.serialize(value));
            column.setTimestamp(System.currentTimeMillis());

            client.insert(CassandraDBUtils.toByteBuffer(SerializationUtils.serialize(key)), parent, column, consistencyLevel);
        } catch (Exception e) {
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public void store(Serializable key, Serializable value) throws ObjectStoreException {
        store(key, value, getDefaultPartition());
    }


    @Override
    public boolean contains(Serializable key, String partition) throws ObjectStoreException {
        try {

            ColumnParent parent = getColumnFamily(partition);

            SlicePredicate predicate = new SlicePredicate();
            predicate.setColumn_names(Collections.singletonList(CassandraDBUtils.toByteBuffer(OBJECT)));

            return client.get_count(
                    CassandraDBUtils.toByteBuffer(CassandraDBUtils.toByteBuffer(SerializationUtils.serialize(key))),
                    parent,
                    predicate,
                    consistencyLevel) != 0;
        } catch (Exception e) {
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public boolean contains(Serializable key) throws ObjectStoreException {
        return contains(key, getDefaultPartition());
    }

    @Override
    public Serializable retrieve(Serializable key, String partition) throws ObjectStoreException {
        try {
            ColumnPath cPath = new ColumnPath();
            cPath.setColumn_family(partition);
            cPath.setColumn(CassandraDBUtils.toByteBuffer(OBJECT));
            ColumnOrSuperColumn result = client.get(
                    CassandraDBUtils.toByteBuffer(CassandraDBUtils.toByteBuffer(SerializationUtils.serialize(key))),
                    cPath,
                    consistencyLevel);
            if (result != null) {
                return (Serializable) SerializationUtils.deserialize(result.getColumn().getValue());
            } else {
                return result;
            }

        } catch (Exception e) {
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public Serializable retrieve(Serializable key) throws ObjectStoreException {
        return retrieve(key, getDefaultPartition());
    }

    @Override
    public Serializable remove(Serializable key, String partition) throws ObjectStoreException {
        try {
            ColumnPath cPath = new ColumnPath();
            cPath.setColumn_family(partition);

            Serializable result = retrieve(key);

            client.remove(CassandraDBUtils.toByteBuffer(SerializationUtils.serialize(key)), cPath,
                    new Date().getTime(), consistencyLevel);
            return result;
        } catch (Exception e) {
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public Serializable remove(Serializable key) throws ObjectStoreException {
        return remove(key, getDefaultPartition());
    }

    @Override
    public List<Serializable> allKeys(String partition) throws ObjectStoreException {
        try {
            List<Serializable> keys = new ArrayList<Serializable>();

            SlicePredicate predicate = new SlicePredicate();
            predicate.setSlice_range(new SliceRange(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0]),
                    false, 100));

            KeyRange keyRange = new KeyRange();
            keyRange.setStart_key(new byte[0]);
            keyRange.setEnd_key(new byte[0]);
            ColumnParent parent = CassandraDBUtils
                    .generateColumnParent(partition);
            List<KeySlice> keySlices = client.get_range_slices(parent, predicate, keyRange, ConsistencyLevel.ONE);

            for (KeySlice slice : keySlices) {
                if (!slice.getColumns().isEmpty()) {
                    keys.add((Serializable) SerializationUtils.deserialize(slice.getKey()));
                }
            }
            return keys;
        } catch (Exception e) {
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public List<Serializable> allKeys() throws ObjectStoreException {
        return allKeys(getDefaultPartition());
    }

    @Override
    public List<String> allPartitions() throws ObjectStoreException {

        try {
            List<String> partitionNames = new ArrayList<String>();
            for (CfDef cfDef : client.describe_keyspace(keyspace).getCf_defs()) {
                partitionNames.add(cfDef.getName());
            }
            return partitionNames;
        } catch (Exception e) {
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public void disposePartition(String partitionName) throws ObjectStoreException {
        try {
            client.truncate(partitionName);
        } catch (Exception e) {
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public void clear(String keyspace) throws ObjectStoreException {
        try {
            client.system_drop_keyspace(keyspace);
        } catch (Exception e) {
            throw new ObjectStoreException(e);
        }
    }

    private ColumnParent getColumnFamily(String name) throws CassandraDBException { // NOSONAR
        boolean columnFamilyExists = false;
        try {
            for (CfDef cfDef : client.describe_keyspace(keyspace).getCf_defs()) {
                if (cfDef.getName().equals(name)) {
                    columnFamilyExists = true;
                }
            }

            if (!columnFamilyExists) {
                CfDef cfDef = new CfDef();
                cfDef.setKeyspace(keyspace);
                cfDef.setName(name);
                client.system_add_column_family(cfDef);
            }

            return CassandraDBUtils
                    .generateColumnParent(name);
        } catch (SchemaDisagreementException e) {
            throw new CassandraDBException(e.getMessage(), e);
        } catch (InvalidRequestException e) {
            throw new CassandraDBException(e.getMessage(), e);
        } catch (TException e) {
            throw new CassandraDBException(e.getMessage(), e);
        } catch (NotFoundException e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    @Override
    public void open(String partition) throws ObjectStoreException {
        open();
    }

    @Override
    public void open() throws ObjectStoreException {
        LOGGER.info("Opening connection");
        try {
            tr = new TFramedTransport(new TSocket(host, port));
            client = CassandraDBUtils.getClient(host, port, keyspace, username, password, tr);
            client.set_keyspace(keyspace);
            LOGGER.debug("Connection created: " + tr);
            tr.open();

        } catch (Exception e) {
            LOGGER.error("Unable to connect to Casssandra DB instance", e);
            throw new ObjectStoreException(e);
        }
    }

    private String getDefaultPartition() {
        if (StringUtils.isBlank(defaultPartitionName)) {
            return FALLBACK_PARTITION_NAME;
        } else {
            final MuleEvent muleEvent = RequestContext.getEvent();
            String parsedPartitionName = context.getExpressionManager().parse(defaultPartitionName, muleEvent);
            LOGGER.debug("PARSED PARTITION NAME: " + parsedPartitionName);
            return parsedPartitionName;
        }
    }


    @Override
    public void close() throws ObjectStoreException {
        tr.close();
    }

    @Override
    public void close(String s) throws ObjectStoreException {
        close();
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public void clear() throws ObjectStoreException {
        this.clear(getDefaultPartition());
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setDefaultPartitionName(String defaultPartitionName) {
        this.defaultPartitionName = defaultPartitionName;
    }

    public void setClient(Cassandra.Client client) {
        this.client = client;
    }
}
