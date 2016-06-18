/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.factory;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;

public class DefaultV2ValuesWriterFactory implements ValuesWriterFactory {

  private ValuesWriterFactoryParams selectionParams;

  @Override
  public void initialize(ValuesWriterFactoryParams params) {
    this.selectionParams = params;
  }

  private Encoding getEncodingForDataPage() {
    return RLE_DICTIONARY;
  }

  private Encoding getEncodingForDictionaryPage() {
    return PLAIN;
  }

  @Override
  public ValuesWriter newValuesWriter(ColumnDescriptor descriptor) {
    switch (descriptor.getType()) {
      case BOOLEAN:
        return getBooleanValuesWriter();
      case FIXED_LEN_BYTE_ARRAY:
        return getFixedLenByteArrayValuesWriter(descriptor);
      case BINARY:
        return getBinaryValuesWriter(descriptor);
      case INT32:
        return getInt32ValuesWriter(descriptor);
      case INT64:
        return getInt64ValuesWriter(descriptor);
      case INT96:
        return getInt96ValuesWriter(descriptor);
      case DOUBLE:
        return getDoubleValuesWriter(descriptor);
      case FLOAT:
        return getFloatValuesWriter(descriptor);
      default:
        throw new IllegalArgumentException("Unknown type " + descriptor.getType());
    }
  }

  private ValuesWriter getBooleanValuesWriter() {
    // no dictionary encoding for boolean
    return new RunLengthBitPackingHybridValuesWriter(1, selectionParams.getInitialCapacity(), selectionParams.getPageSize(), selectionParams.getAllocator());
  }

  private ValuesWriter getFixedLenByteArrayValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new DeltaByteArrayWriter(selectionParams.getInitialCapacity(), selectionParams.getPageSize(), selectionParams.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, selectionParams, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }

  private ValuesWriter getBinaryValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new DeltaByteArrayWriter(selectionParams.getInitialCapacity(), selectionParams.getPageSize(), selectionParams.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, selectionParams, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }

  private ValuesWriter getInt32ValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new DeltaBinaryPackingValuesWriterForInteger(selectionParams.getInitialCapacity(), selectionParams.getPageSize(), selectionParams.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, selectionParams, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }

  private ValuesWriter getInt64ValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new DeltaBinaryPackingValuesWriterForLong(selectionParams.getInitialCapacity(), selectionParams.getPageSize(), selectionParams.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, selectionParams, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }

  private ValuesWriter getInt96ValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new FixedLenByteArrayPlainValuesWriter(12, selectionParams.getInitialCapacity(), selectionParams.getPageSize(), selectionParams.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, selectionParams, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }

  private ValuesWriter getDoubleValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new PlainValuesWriter(selectionParams.getInitialCapacity(), selectionParams.getPageSize(), selectionParams.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, selectionParams, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }

  private ValuesWriter getFloatValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new PlainValuesWriter(selectionParams.getInitialCapacity(), selectionParams.getPageSize(), selectionParams.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, selectionParams, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }
}
