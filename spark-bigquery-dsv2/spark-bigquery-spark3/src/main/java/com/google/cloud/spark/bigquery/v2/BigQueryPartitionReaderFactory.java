package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.common.GenericArrowBigQueryInputPartitionHelper;
import com.google.cloud.spark.bigquery.common.GenericBigQuerySchemaHelper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class BigQueryPartitionReaderFactory implements PartitionReaderFactory, Serializable {
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
        if (partition instanceof ArrowInputPartition) {

            GenericArrowBigQueryInputPartitionHelper bqInputPartitionHelper =
                    new GenericArrowBigQueryInputPartitionHelper();
            // using generic helper class from dsv 2 parent library to create tracer,read row request object
            //  for each inputPartition reader
            BigQueryStorageReadRowsTracer tracer =
                    bqInputPartitionHelper.getBQTracerByStreamNames(
                            ((ArrowInputPartition) partition).getTracerFactory(), ((ArrowInputPartition) partition).getStreamNames());
            List<ReadRowsRequest.Builder> readRowsRequests =
                    bqInputPartitionHelper.getListOfReadRowsRequestsByStreamNames(((ArrowInputPartition) partition).getStreamNames());

            ReadRowsHelper readRowsHelper =
                    new ReadRowsHelper(
                            ((ArrowInputPartition) partition).getBigQueryReadClientFactory(), readRowsRequests, ((ArrowInputPartition) partition).getOptions());
            tracer.startStream();
            // iterator to read data from bigquery read rows object
            Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
            return new ArrowColumnBatchPartitionColumnarBatchReader(
                    readRowsResponses,
                    ((ArrowInputPartition) partition).getSerializedArrowSchema(),
                    readRowsHelper,
                    ((ArrowInputPartition) partition).getSelectedFields(),
                    tracer,
                    ((ArrowInputPartition) partition).getUserProvidedSchema().toJavaUtil(),
                    ((ArrowInputPartition) partition).getOptions().numBackgroundThreads());

        } else {
            throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
        }

    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return helper.isEnableBatchRead(readSessionCreatorConfig, schema);
    }
}
