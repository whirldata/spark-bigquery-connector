package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.spark.bigquery.common.GenericArrowInputPartition;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.connector.read.InputPartition;

import java.util.List;

public class ArrowInputPartition extends GenericArrowInputPartition implements InputPartition {
    public ArrowInputPartition(BigQueryReadClientFactory bigQueryReadClientFactory, BigQueryTracerFactory tracerFactory, List<String> names, ReadRowsHelper.Options options, ImmutableList<String> selectedFields, ReadSessionResponse readSessionResponse) {
        super(bigQueryReadClientFactory, tracerFactory, names, options, selectedFields, readSessionResponse);
    }
}
