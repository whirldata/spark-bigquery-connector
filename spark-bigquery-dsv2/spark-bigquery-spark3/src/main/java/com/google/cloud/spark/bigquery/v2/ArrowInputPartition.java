package com.google.cloud.spark.bigquery.v2;

import static com.google.common.base.Optional.fromJavaUtil;

import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.spark.bigquery.common.GenericArrowInputPartition;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

public class ArrowInputPartition  implements InputPartition {
  public com.google.common.base.Optional<StructType> getUserProvidedSchema() {
    return userProvidedSchema;
  }

  private final com.google.common.base.Optional<StructType> userProvidedSchema;

  private GenericArrowInputPartition arrowInputPartitionHelper;
  public ArrowInputPartition(
      BigQueryReadClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      String names,
      ReadRowsHelper.Options options,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse,
      Optional<StructType> userProvidedSchema) {
    arrowInputPartitionHelper=new GenericArrowInputPartition(
        bigQueryReadClientFactory,
        tracerFactory,
        names,
        options,
        selectedFields,
        readSessionResponse);
    this.userProvidedSchema = fromJavaUtil(userProvidedSchema);
  }
}
