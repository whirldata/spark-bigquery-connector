package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;

public class CustomDataSourceRunner {
    public static void main(String[] args) {
        SparkSession sparkSession = new CustomDataSourceRunner().getDefaultSparkSessionOrCreate();
        Dataset<Row> simpleDf = sparkSession.read()
                .format("com.google.cloud.spark.bigquery.custom.CustomTableProvider")
                .load();
        simpleDf.show();
    }

    private SparkSession getDefaultSparkSessionOrCreate() {
        scala.Option<SparkSession> defaultSparkSession = SparkSession.getActiveSession();
        if (defaultSparkSession.isDefined()) {
            return defaultSparkSession.get();
        }
        return SparkSession.builder().appName("spark-bigquery-connector").master("local[*]").getOrCreate();
    }
}
