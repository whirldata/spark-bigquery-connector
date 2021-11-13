/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDataSourceWriterModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.IOException;
import java.io.Serializable;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

class BigQueryDataSourceWriterModule implements Module, Serializable {
  private GenericBigQueryDataSourceWriterModule dataSourceWriterModuleHelper;

  BigQueryDataSourceWriterModule(String writeUUID, StructType sparkSchema, SaveMode mode) {
    this.dataSourceWriterModuleHelper =
        new GenericBigQueryDataSourceWriterModule(writeUUID, sparkSchema, mode);
  }

  @Override
  public void configure(Binder binder) {
    // empty
  }

  @Singleton
  @Provides
  public BigQueryIndirectDataSourceWriter provideDataSourceWriter(
      BigQueryClient bigQueryClient, SparkBigQueryConfig config, SparkSession spark)
      throws IOException {
    this.dataSourceWriterModuleHelper.createIntermediateCleaner(
        config, spark.sparkContext().hadoopConfiguration(), spark.sparkContext().applicationId());
    // based on pmkc's suggestion at https://git.io/JeWRt
    return new BigQueryIndirectDataSourceWriter(
        bigQueryClient,
        config,
        spark.sparkContext().hadoopConfiguration(),
        this.dataSourceWriterModuleHelper.getSparkSchema(),
        this.dataSourceWriterModuleHelper.getWriteUUID(),
        this.dataSourceWriterModuleHelper.getMode(),
        this.dataSourceWriterModuleHelper.getGcsPath(),
        this.dataSourceWriterModuleHelper.getIntermediateDataCleaner());
  }
}
