/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDataSourceWriterModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.Map;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class BigQueryTableModule implements Module {
  private Map<String, String> properties;
  private StructType schema;
  private GenericBigQueryDataSourceWriterModule dataSourceWriterModuleHelper;
  private BigQueryClient bigQueryClient;

  @Override
  public void configure(Binder binder) {
    // empty
  }

  public BigQueryTableModule(StructType schema, Map<String, String> properties) {
    this.schema = schema;
    this.properties = properties;
  }

  @Singleton
  @Provides
  public BigQueryTable provideDataSource(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      SparkBigQueryConfig config,
      SparkSession sparkSession)
      throws AnalysisException {
    return new BigQueryTable(
        config.getSchema().get(), null, this.properties, bigQueryClient, config, sparkSession);
  }
}
