package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.common.GenericBigQueryEmptyProjectionInputPartition;
import org.apache.spark.sql.connector.read.InputPartition;

public class BigQueryEmptyProjectInputPartition extends GenericBigQueryEmptyProjectionInputPartition implements InputPartition
{
    public BigQueryEmptyProjectInputPartition(int partitionSize) {
        super(partitionSize);
    }
}
