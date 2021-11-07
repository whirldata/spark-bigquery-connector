package com.google.cloud.spark.bigquery.v2;

import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Set;

public class BigQueryTable implements Table, SupportsRead, SupportsWrite {
    private static final Set<TableCapability> CAPABILITIES = ImmutableSet.of(
            TableCapability.BATCH_READ,
            TableCapability.BATCH_WRITE);
    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return null;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return null;
    }

    @Override
    public String name() {
        return this.getClass().getName();
    }

    @Override
    public StructType schema() {
        return null;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return CAPABILITIES;
    }
}
