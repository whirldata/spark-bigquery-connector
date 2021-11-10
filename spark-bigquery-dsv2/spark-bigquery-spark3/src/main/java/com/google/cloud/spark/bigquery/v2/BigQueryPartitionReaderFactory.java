package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.common.GenericArrowBigQueryInputPartitionHelper;
import com.google.cloud.spark.bigquery.common.GenericBigQuerySchemaHelper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;
import java.util.List;

public class BigQueryPartitionReaderFactory implements PartitionReaderFactory {

    private GenericBigQuerySchemaHelper schemaHelper;

    BigQueryPartitionReaderFactory() {
        schemaHelper = new GenericBigQuerySchemaHelper();
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        System.out.println(partition.getClass().getName());
        if (partition instanceof BigQueryInputPartition || partition instanceof BigQueryEmptyProjectInputPartition) {
            ReadRowsRequest.Builder readRowsRequest =
                    ReadRowsRequest.newBuilder().setReadStream(((BigQueryInputPartition) partition).getStreamName());
            ReadRowsHelper readRowsHelper =
                    new ReadRowsHelper(((BigQueryInputPartition) partition).getBigQueryReadClientFactory(), readRowsRequest, ((BigQueryInputPartition) partition).getOptions());
            Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
            return new BigQueryInputPartitionReader(readRowsResponses, ((BigQueryInputPartition) partition).getConverter(), readRowsHelper);
        } else {
            throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
        }
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        if (partition instanceof ArrowInputPartition) {

            GenericArrowBigQueryInputPartitionHelper bqInputPartitionHelper =
                    new GenericArrowBigQueryInputPartitionHelper();
            // using generic helper class from dsv 2 parent library to create tracer,read row request
            // object
            //  for each inputPartition reader
            BigQueryStorageReadRowsTracer tracer =
                    bqInputPartitionHelper.getBQTracerByStreamNames(
                            ((ArrowInputPartition) partition).getTracerFactory(),
                            ((ArrowInputPartition) partition).getStreamNames());
            List<ReadRowsRequest.Builder> readRowsRequests =
                    bqInputPartitionHelper.getListOfReadRowsRequestsByStreamNames(
                            ((ArrowInputPartition) partition).getStreamNames());

            ReadRowsHelper readRowsHelper =
                    new ReadRowsHelper(
                            ((ArrowInputPartition) partition).getBigQueryReadClientFactory(),
                            readRowsRequests,
                            ((ArrowInputPartition) partition).getOptions());
            tracer.startStream();
            // iterator to read data from bigquery read rows object
            Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
            return new ArrowColumnBatchPartitionReader(
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
        return false;
        //return helper.isEnableBatchRead(readSessionCreatorConfig, schema);
    }
}
