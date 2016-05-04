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
package org.apache.parquet.column.values;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.Test;

import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test few configurations for creating values writers based on encoding overrides.
 */
public class ValuesWriterFactoryEncodingOverridesTest {

  // test overrides for boolean
  @Test
  public void testBooleanEncodingOverride() {
    doTestValueWriter(
      PrimitiveTypeName.BOOLEAN,
      WriterVersion.PARQUET_2_0,
      generateEncodingOverrides(PrimitiveTypeName.BOOLEAN, Encoding.RLE),
      RunLengthBitPackingHybridValuesWriter.class);
  }

  // test overrides for fixed len byte array
  @Test
  public void testFixedLenByteArrayEncodingOverride() {
    doTestValueWriter(
      PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
      WriterVersion.PARQUET_2_0,
      generateEncodingOverrides(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Encoding.DELTA_BYTE_ARRAY),
      DeltaByteArrayWriter.class);
  }

  // test overrides for binary
  @Test
  public void testBinaryEncodingOverride() {
    doTestValueWriter(
      PrimitiveTypeName.BINARY,
      WriterVersion.PARQUET_2_0,
      generateEncodingOverrides(PrimitiveTypeName.BINARY, Encoding.DELTA_BYTE_ARRAY),
      DeltaByteArrayWriter.class);
  }

  // test overrides for int32
  @Test
  public void testInt32EncodingOverride() {
    doTestValueWriter(
      PrimitiveTypeName.INT32,
      WriterVersion.PARQUET_2_0,
      generateEncodingOverrides(PrimitiveTypeName.INT32, Encoding.DELTA_BINARY_PACKED),
      DeltaBinaryPackingValuesWriterForInteger.class);
  }

  // test overrides for int64
  @Test
  public void testInt64EncodingOverride() {
    doTestValueWriter(
      PrimitiveTypeName.INT64,
      WriterVersion.PARQUET_2_0,
      generateEncodingOverrides(PrimitiveTypeName.INT64, Encoding.DELTA_BINARY_PACKED),
      DeltaBinaryPackingValuesWriterForLong.class);
  }

  // test overrides for int96
  @Test
  public void testInt96EncodingOverride() {
    doTestValueWriter(
      PrimitiveTypeName.INT96,
      WriterVersion.PARQUET_2_0,
      generateEncodingOverrides(PrimitiveTypeName.INT96, Encoding.PLAIN),
      FixedLenByteArrayPlainValuesWriter.class);
  }

  // test overrides for double
  @Test
  public void testDoubleEncodingOverride() {
    doTestValueWriter(
      PrimitiveTypeName.DOUBLE,
      WriterVersion.PARQUET_2_0,
      generateEncodingOverrides(PrimitiveTypeName.DOUBLE, Encoding.PLAIN),
      PlainValuesWriter.class);
  }

  // test overrides for float
  @Test
  public void testFloatEncodingOverride() {
    doTestValueWriter(
      PrimitiveTypeName.FLOAT,
      WriterVersion.PARQUET_2_0,
      generateEncodingOverrides(PrimitiveTypeName.FLOAT, Encoding.PLAIN),
      PlainValuesWriter.class);
  }

  // test that we crib if the initial encoding isn't fallback friendly
  @Test(expected = IllegalArgumentException.class)
  public void testIncorrectInitialEncodingOverride() {
    doTestValueWriter(
      PrimitiveTypeName.INT32,
      WriterVersion.PARQUET_2_0,
      generateEncodingOverrides(PrimitiveTypeName.INT32, Encoding.DELTA_BINARY_PACKED, Encoding.PLAIN),
      DeltaBinaryPackingValuesWriterForInteger.class);
  }

  // test that dictionary encoding with a fallback works in V1
  @Test
  public void testEncodingOverrideWithFallback_V1() {
    doTestValueWriter(
      PrimitiveTypeName.INT32,
      WriterVersion.PARQUET_1_0,
      generateEncodingOverrides(PrimitiveTypeName.INT32, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN),
      PlainIntegerDictionaryValuesWriter.class,
      PlainValuesWriter.class);
  }

  // test that RLE dictionary encoding with a fallback works in V2
  @Test
  public void testRLEDictionaryEncodingOverrideWithFallback_V2() {
    doTestValueWriter(
      PrimitiveTypeName.INT32,
      WriterVersion.PARQUET_2_0,
      generateEncodingOverrides(PrimitiveTypeName.INT32, Encoding.RLE_DICTIONARY, Encoding.PLAIN),
      PlainIntegerDictionaryValuesWriter.class,
      PlainValuesWriter.class);
  }

  // test that we crib if someone tries to use RLE_DICTIONARY encoding for booleans
  @Test(expected = ParquetEncodingException.class)
  public void testRLEDictionaryNotSupportedBoolean() {
    doTestValueWriter(
      PrimitiveTypeName.BOOLEAN,
      WriterVersion.PARQUET_2_0,
      generateEncodingOverrides(PrimitiveTypeName.BOOLEAN, Encoding.RLE_DICTIONARY, Encoding.PLAIN),
      PlainValuesWriter.class);
  }

  // test that we crib if someone tries to use a V2 encoding in V1
  @Test(expected = ParquetEncodingException.class)
  public void testEncodingNotSupportedForVersion() {
    doTestValueWriter(
      PrimitiveTypeName.INT32,
      WriterVersion.PARQUET_1_0,
      generateEncodingOverrides(PrimitiveTypeName.INT32, Encoding.DELTA_BINARY_PACKED),
      DeltaBinaryPackingValuesWriterForInteger.class);
  }

  private Map<PrimitiveTypeName, List<Encoding>> generateEncodingOverrides(PrimitiveTypeName typeName, Encoding ... encodings) {
    List<Encoding> encodingList = Lists.newArrayList(encodings);
    return ImmutableMap.of(typeName, encodingList);
  }

  private void doTestValueWriter(PrimitiveTypeName typeName,
                                 WriterVersion writerVersion,
                                 Map<PrimitiveTypeName, List<Encoding>> encodingOverrides,
                                 Class<? extends ValuesWriter> expectedValueWriterClass) {
    ColumnDescriptor mockPath = getMockColumn(typeName);
    ValuesWriterFactory factory = getFactory(writerVersion, encodingOverrides);
    ValuesWriter writer = factory.newValuesWriter(mockPath);

    validateWriterType(writer, expectedValueWriterClass);
  }

  private void doTestValueWriter(PrimitiveTypeName typeName,
                                 WriterVersion writerVersion,
                                 Map<PrimitiveTypeName, List<Encoding>> encodingOverrides,
                                 Class<? extends ValuesWriter> initialValueWriterClass,
                                 Class<? extends ValuesWriter> fallbackValueWriterClass) {
    ColumnDescriptor mockPath = getMockColumn(typeName);
    ValuesWriterFactory factory = getFactory(writerVersion, encodingOverrides);
    ValuesWriter writer = factory.newValuesWriter(mockPath);

    validateFallbackWriter(writer, initialValueWriterClass, fallbackValueWriterClass);
  }

  private ColumnDescriptor getMockColumn(PrimitiveTypeName typeName) {
    ColumnDescriptor mockPath = mock(ColumnDescriptor.class);
    when(mockPath.getType()).thenReturn(typeName);
    return mockPath;
  }

  private ValuesWriterFactory getFactory(WriterVersion writerVersion,
                                         Map<PrimitiveTypeName, List<Encoding>> encodingOverrides) {
    return new ValuesWriterFactory(
      writerVersion,
      128,
      ParquetProperties.DEFAULT_PAGE_SIZE,
      null,
      0,
      true,
      encodingOverrides);
  }

  private void validateWriterType(ValuesWriter writer, Class<? extends ValuesWriter> valuesWriterClass) {
    assertTrue("Not instance of: " + valuesWriterClass.getName() + " is: " + writer.getClass(),
               valuesWriterClass.isInstance(writer));
  }

  private void validateFallbackWriter(ValuesWriter writer, Class<? extends ValuesWriter> initialWriterClass, Class<? extends ValuesWriter> fallbackWriterClass) {
    validateWriterType(writer, FallbackValuesWriter.class);

    FallbackValuesWriter wr = (FallbackValuesWriter) writer;
    validateWriterType(wr.initialWriter, initialWriterClass);
    validateWriterType(wr.fallBackWriter, fallbackWriterClass);
  }
}
