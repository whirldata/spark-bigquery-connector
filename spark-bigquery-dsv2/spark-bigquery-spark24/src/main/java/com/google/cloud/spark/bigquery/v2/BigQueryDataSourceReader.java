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

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDataSourceReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.Statistics;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDataSourceReader
    implements DataSourceReader,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsReportStatistics,
        SupportsScanColumnarBatch {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryDataSourceReader.class);
  private GenericBigQueryDataSourceReader dataSourceReaderHelper;
  private static Statistics UNKNOWN_STATISTICS =
      new Statistics() {

        @Override
        public OptionalLong sizeInBytes() {
          return OptionalLong.empty();
        }

        @Override
        public OptionalLong numRows() {
          return OptionalLong.empty();
        }
      };

  public BigQueryDataSourceReader(
      TableInfo table,
      BigQueryClient bigQueryClient,
      BigQueryReadClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      ReadSessionCreatorConfig readSessionCreatorConfig,
      Optional<String> globalFilter,
      Optional<StructType> schema,
      String applicationId) {
    this.dataSourceReaderHelper =
        new GenericBigQueryDataSourceReader(
            table,
            readSessionCreatorConfig,
            bigQueryClient,
            bigQueryReadClientFactory,
            tracerFactory,
            globalFilter,
            schema,
            applicationId);
  }

  @Override
  public StructType readSchema() {
    // TODO: rely on Java code
    return this.dataSourceReaderHelper.readSchema();
  }

  @Override
  public boolean enableBatchRead() {
    return this.dataSourceReaderHelper.enableBatchRead();
  }

  @Override
  public List<InputPartition<InternalRow>> planInputPartitions() {
    if (this.dataSourceReaderHelper.isEmptySchema()) {
      // create empty projection
      return createEmptyProjectionPartitions();
    }
    this.dataSourceReaderHelper.createReadSession(false);
    logger.info(
        "Created read session for {}: {} for application id: {}",
        this.dataSourceReaderHelper.getTableId().toString(),
        this.dataSourceReaderHelper.getReadSession().getName(),
        this.dataSourceReaderHelper.getApplicationId());
    return this.dataSourceReaderHelper.getReadSession().getStreamsList().stream()
        .map(
            stream ->
                new BigQueryInputPartition(
                    this.dataSourceReaderHelper.getBigQueryReadClientFactory(),
                    stream.getName(),
                    this.dataSourceReaderHelper
                        .getReadSessionCreatorConfig()
                        .toReadRowsHelperOptions(),
                    this.dataSourceReaderHelper.createConverter()))
        .collect(Collectors.toList());
  }

  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
    if (!this.dataSourceReaderHelper.enableBatchRead()) {
      throw new IllegalStateException("Batch reads should not be enabled");
    }
    this.dataSourceReaderHelper.createReadSession(true);
    logger.info(
        "Created read session for {}: {} for application id: {}",
        this.dataSourceReaderHelper.getTableId().toString(),
        this.dataSourceReaderHelper.getReadSession().getName(),
        this.dataSourceReaderHelper.getApplicationId());

    if (this.dataSourceReaderHelper.getSelectedFields().isEmpty()) {
      // means select *
      this.dataSourceReaderHelper.emptySchemaForPartition();
    }

    ImmutableList<String> partitionSelectedFields = this.dataSourceReaderHelper.getSelectedFields();
    return Streams.stream(
            Iterables.partition(
                this.dataSourceReaderHelper.getReadSession().getStreamsList(),
                this.dataSourceReaderHelper.getReadSessionCreatorConfig().streamsPerPartition()))
        .map(
            streams ->
                new ArrowInputPartition(
                    this.dataSourceReaderHelper.getBigQueryReadClientFactory(),
                    this.dataSourceReaderHelper.getBigQueryTracerFactory(),
                    streams.stream()
                        .map(ReadStream::getName)
                        // This formulation is used to guarantee a serializable list.
                        .collect(Collectors.toCollection(ArrayList::new)),
                    this.dataSourceReaderHelper
                        .getReadSessionCreatorConfig()
                        .toReadRowsHelperOptions(),
                    partitionSelectedFields,
                    this.dataSourceReaderHelper.getReadSessionResponse(),
                    this.dataSourceReaderHelper.getUserProvidedSchema()))
        .collect(Collectors.toList());
  }

  List<InputPartition<InternalRow>> createEmptyProjectionPartitions() {
    this.dataSourceReaderHelper.createEmptyProjectionPartitions();
    InputPartition<InternalRow>[] partitions =
        IntStream.range(0, this.dataSourceReaderHelper.getPartitionsCount())
            .mapToObj(
                ignored ->
                    new BigQueryEmptyProjectionInputPartition(
                        this.dataSourceReaderHelper.getPartitionSize()))
            .toArray(BigQueryEmptyProjectionInputPartition[]::new);
    partitions[0] =
        new BigQueryEmptyProjectionInputPartition(
            this.dataSourceReaderHelper.getFirstPartitionSize());
    return ImmutableList.copyOf(partitions);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    return this.dataSourceReaderHelper.pushFilters(filters);
  }

  @Override
  public Filter[] pushedFilters() {
    return this.dataSourceReaderHelper.getPushedFilters();
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.dataSourceReaderHelper.pruneColumns(requiredSchema);
  }

  @Override
  public Statistics estimateStatistics() {
    return this.dataSourceReaderHelper.getTable().getDefinition().getType()
            == TableDefinition.Type.TABLE
        ? new StandardTableStatistics(this.dataSourceReaderHelper.getTable().getDefinition())
        : UNKNOWN_STATISTICS;
  }
}

class StandardTableStatistics implements Statistics {

  private StandardTableDefinition tableDefinition;

  public StandardTableStatistics(StandardTableDefinition tableDefinition) {
    this.tableDefinition = tableDefinition;
  }

  @Override
  public OptionalLong sizeInBytes() {
    return OptionalLong.of(tableDefinition.getNumBytes());
  }

  @Override
  public OptionalLong numRows() {
    return OptionalLong.of(tableDefinition.getNumRows());
  }
}
