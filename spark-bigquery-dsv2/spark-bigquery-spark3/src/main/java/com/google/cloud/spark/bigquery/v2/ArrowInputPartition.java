package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.spark.bigquery.common.GenericArrowInputPartition;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Optional.fromJavaUtil;

public class ArrowInputPartition extends GenericArrowInputPartition
        implements InputPartition {
    private final com.google.common.base.Optional<StructType> userProvidedSchema;
    public ArrowInputPartition(
            BigQueryReadClientFactory bigQueryReadClientFactory,
            BigQueryTracerFactory tracerFactory,
            List<String> names,
            ReadRowsHelper.Options options,
            ImmutableList<String> selectedFields,
            ReadSessionResponse readSessionResponse,
            Optional<StructType> userProvidedSchema) {
        super(
                bigQueryReadClientFactory,
                tracerFactory,
                names,
                options,
                selectedFields,
                readSessionResponse);
        this.userProvidedSchema = fromJavaUtil(userProvidedSchema);
    }
}


