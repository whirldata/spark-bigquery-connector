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

import com.google.cloud.spark.bigquery.common.GenericAvroIntermediateRecordWriter;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroIntermediateRecordWriter extends GenericAvroIntermediateRecordWriter
    implements IntermediateRecordWriter {

  AvroIntermediateRecordWriter(Schema schema, OutputStream outputStream) throws IOException {
    super(schema, outputStream);
  }

  @Override
  public void write(GenericRecord record) throws IOException {
    getDataFileWriter().append(record);
  }

  @Override
  public void close() throws IOException {
    try {
      getDataFileWriter().flush();
    } finally {
      getDataFileWriter().close();
    }
  }
}
