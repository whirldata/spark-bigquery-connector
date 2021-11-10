package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryConnectorModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class BigQueryDataSourceV2 implements DataSourceRegister, TableProvider {

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return null;
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    Map<String, String> props = new HashMap<>(properties);
    Injector injector;
    //    if (schema != null)
    injector = createInjector(schema, props, new BQBatchTableModule(schema, props));
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    TableInfo table = bigQueryClient.getTable(config.getTableId());
    if (table != null) {
      schema = SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table));
      injector = createInjector(schema, props, new BQBatchTableModule(schema, props));
    }
    return injector.getInstance(BigQueryTable.class);
  }

  @Override
  public String shortName() {
    return "bigquery";
  }

  public SparkSession getDefaultSparkSessionOrCreate() {
    scala.Option<SparkSession> defaultSpareSession = SparkSession.getActiveSession();
    if (defaultSpareSession.isDefined()) {
      return defaultSpareSession.get();
    }
    return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
  }

  // This method is used to create injection by providing
  public Injector createInjector(StructType schema, Map<String, String> options, Module module) {
    SparkSession spark = getDefaultSparkSessionOrCreate();
    return Guice.createInjector(
        new BigQueryClientModule(),
        new SparkBigQueryConnectorModule(
            spark, options, Optional.ofNullable(schema), DataSourceVersion.V2),
        module);
  }
}
