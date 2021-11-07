package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.spark.bigquery.common.BQSparkFilterHelper;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class BigQueryScanBuilder implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
    TableInfo table;
    ReadSessionCreatorConfig readSessionCreatorConfig;
    private Optional<StructType> schema;
    private BQSparkFilterHelper filterHelper;

    BigQueryScanBuilder(TableInfo table, ReadSessionCreatorConfig readSessionCreatorConfig, Optional<StructType> schema) {
        this.table = table;
        this.readSessionCreatorConfig = readSessionCreatorConfig;
        this.schema = schema;
        filterHelper = new BQSparkFilterHelper(table, readSessionCreatorConfig);
    }


    @Override
    public Filter[] pushFilters(Filter[] filters) {
        return filterHelper.pushFilters(filters, readSessionCreatorConfig, filterHelper.getFields());
    }

    @Override
    public Filter[] pushedFilters() {
        return filterHelper.getPushedFilters();
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.schema = Optional.ofNullable(requiredSchema);
    }

    @Override
    public Scan build() {
        return null;
    }
}
