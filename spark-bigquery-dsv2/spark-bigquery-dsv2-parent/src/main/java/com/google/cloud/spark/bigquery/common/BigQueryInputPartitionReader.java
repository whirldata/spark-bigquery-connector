package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.io.IOException;
import java.util.Iterator;

public class BigQueryInputPartitionReader extends GenericBigQueryInputPartitionReader implements InputPartitionReader<InternalRow> {
    public BigQueryInputPartitionReader
            (Iterator<ReadRowsResponse> readRowsResponses,
             ReadRowsResponseToInternalRowIteratorConverter converter,
             ReadRowsHelper readRowsHelper,
             Iterator<InternalRow> rows, InternalRow currentRow) {
        super(readRowsResponses, converter, readRowsHelper, rows, currentRow);
    }


    @Override
    public boolean next() throws IOException {
        return super.next();
    }

    @Override
    public InternalRow get() {

        return super.getCurrentRow();
    }

    @Override
    public void close() throws IOException {
        super.getReadRowsHelper().close();
    }
}
