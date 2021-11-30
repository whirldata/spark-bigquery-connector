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
package com.google.cloud.spark.bigquery.common;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.StructType;

public class GenericBigQueryIndirectDataWriterFactory implements Serializable {
  String gcsDirPath;
  String avroSchemaJson;
  String uri;
  Path path;
  Schema avroSchema;
  SerializableConfiguration conf;
  StructType sparkSchema;
  FileSystem fs;
  IntermediateRecordWriter intermediateRecordWriter;

  public GenericBigQueryIndirectDataWriterFactory(
      SerializableConfiguration conf,
      String gcsDirPath,
      StructType sparkSchema,
      String avroSchemaJson) {
    this.conf = conf;
    this.gcsDirPath = gcsDirPath;
    this.sparkSchema = sparkSchema;
    this.avroSchemaJson = avroSchemaJson;
  }

  public String getAvroSchemaJson() {
    return this.avroSchemaJson;
  }

  public String getGcsDirPath() {
    return this.gcsDirPath;
  }

  public Path getPath() {
    return path;
  }

  public Schema getAvroSchema() {
    return avroSchema;
  }

  public FileSystem getFs() {
    return fs;
  }

  public StructType getSparkSchema() {
    return sparkSchema;
  }

  public IntermediateRecordWriter getIntermediateRecordWriter() {
    return intermediateRecordWriter;
  }

  public String getUri() {
    return uri;
  }

  public SerializableConfiguration getConf() {
    return conf;
  }

  public void enableDataWriter(int partitionId, long taskId, long epochId) throws IOException {
    this.avroSchema = new Schema.Parser().parse(this.avroSchemaJson);
    UUID uuid;
    if (epochId != -1L) {
      uuid = new UUID(taskId, epochId);
    } else {
      uuid = new UUID(taskId, 0L);
    }
    this.uri = String.format("%s/part-%06d-%s.avro", this.gcsDirPath, partitionId, uuid);
    this.path = new Path(uri);
    this.fs = this.path.getFileSystem(conf.get());
    this.intermediateRecordWriter =
        new GenericAvroIntermediateRecordWriter(this.avroSchema, fs.create(this.path));
  }
}
