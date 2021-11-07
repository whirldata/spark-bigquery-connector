package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.common.GenericBigQuerySchemaHelper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import java.util.Iterator;
import java.util.Optional;

public class BigQueryPartitionReaderFactory implements PartitionReaderFactory {
    private Iterator<ReadRowsResponse> readRowsResponses;
    private ReadRowsResponseToInternalRowIteratorConverter converter;
    private ReadRowsHelper readRowsHelper;
    private Optional<StructType> schema;
    private TableInfo table;
    private ReadSessionCreatorConfig readSessionCreatorConfig;
    private GenericBigQuerySchemaHelper helper;

    BigQueryPartitionReaderFactory(
            Iterator<ReadRowsResponse> readRowsResponses,
            ReadRowsResponseToInternalRowIteratorConverter converter,
            ReadRowsHelper readRowsHelper, Optional<StructType> schema, TableInfo table, ReadSessionCreatorConfig readSessionCreatorConfig) {
        this.readRowsResponses = readRowsResponses;
        this.converter = converter;
        this.readRowsHelper = readRowsHelper;
        this.schema = schema;
        this.readSessionCreatorConfig = readSessionCreatorConfig;
        new GenericBigQuerySchemaHelper();
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        if (partition instanceof BigQueryInputPartition) {
            return new BigQueryInputPartitionReader(readRowsResponses,
                    converter,
                    readRowsHelper);
        } else {
            throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
        }
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        return PartitionReaderFactory.super.createColumnarReader(partition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return helper.isEnableBatchRead(readSessionCreatorConfig, schema);
    }
}
