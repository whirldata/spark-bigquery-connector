package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.common.GenericBigQueryEmptyProjectionInputPartition;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

public class BigQueryEmptyProjectInputPartition extends GenericBigQueryEmptyProjectionInputPartition
    implements InputPartition, Serializable {
  public BigQueryEmptyProjectInputPartition(int partitionSize) {
    super(partitionSize);
  }
}
