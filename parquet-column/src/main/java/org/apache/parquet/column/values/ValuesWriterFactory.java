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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.column.Encoding.GetValuesWriterParams;

public class ValuesWriterFactory {

  private final WriterVersion writerVersion;

  private final boolean enableDictionary;
  private final int initialSlabSize;
  private final int pageSizeThreshold;
  private final ByteBufferAllocator allocator;
  private final int dictionaryPageSizeThreshold;
  private final Map<PrimitiveTypeName, List<Encoding>> encodingOverrides;

  public ValuesWriterFactory(WriterVersion writerVersion, int initialSlabSize, int pageSizeThreshold,
                             ByteBufferAllocator allocator, int dictionaryPageSizeThreshold,
                             boolean enableDictionary,
                             Map<PrimitiveTypeName, List<Encoding>> encodingOverrides) {
    this.writerVersion = writerVersion;
    this.initialSlabSize = initialSlabSize;
    this.pageSizeThreshold = pageSizeThreshold;
    this.allocator = allocator;
    this.dictionaryPageSizeThreshold = dictionaryPageSizeThreshold;
    this.enableDictionary = enableDictionary;
    this.encodingOverrides = encodingOverrides;
  }

  public ValuesWriter newValuesWriter(ColumnDescriptor path) {
    if ( isTypeEncodingOverridden(path) ) {
      return getOverriddenValuesWriter(path, getBitWidth(path));
    }

    switch (path.getType()) {
      case BOOLEAN:
        return getBooleanValuesWriter(path);
      case FIXED_LEN_BYTE_ARRAY:
        return getFixedLenByteArrayValuesWriter(path);
      case BINARY:
        return getBinaryValuesWriter(path);
      case INT32:
        return getInt32ValuesWriter(path);
      case INT64:
        return getInt64ValuesWriter(path);
      case INT96:
        return getInt96ValuesWriter(path);
      case DOUBLE:
        return getDoubleValuesWriter(path);
      case FLOAT:
        return getFloatValuesWriter(path);
      default:
        throw new IllegalArgumentException("Unknown type " + path.getType());
    }
  }

  /**
   * Returns true if there are encoding overrides specified for the type.
   */
  private boolean isTypeEncodingOverridden(ColumnDescriptor path) {
    List<Encoding> overrides = encodingOverrides.get(path.getType());
    return ( overrides != null && ! (overrides.isEmpty()) );
  }

  /**
   * Return the bit width to use for choosing encoding override value writer.
   * For some of the types, the bit width isn't used to create value writers so
   * we default to 0 for them.
   */
  private int getBitWidth(ColumnDescriptor path) {
    switch (path.getType()) {
      case BOOLEAN:
        return 1;
      case FIXED_LEN_BYTE_ARRAY:
        return path.getTypeLength();
      case INT96:
        return 12;
      default:
        return 0;
    }
  }

  private ValuesWriter getOverriddenValuesWriter(ColumnDescriptor path, int bitWidth) {
    List<Encoding> overrides = encodingOverrides.get(path.getType());

    Iterator<Encoding> encodingIterator = overrides.iterator();
    Encoding firstEnc = encodingIterator.next();

    GetValuesWriterParams firstEncParams =
      new GetValuesWriterParams(path, writerVersion, bitWidth, initialSlabSize, pageSizeThreshold, allocator, dictionaryPageSizeThreshold);
    ValuesWriter firstWriter = firstEnc.getValuesWriter(firstEncParams);

    if (encodingIterator.hasNext()) {
      Encoding secondEnc = encodingIterator.next();
      GetValuesWriterParams secondEncParams =
        new GetValuesWriterParams(path, writerVersion, bitWidth, initialSlabSize, pageSizeThreshold, allocator, dictionaryPageSizeThreshold);
      ValuesWriter secondWriter = secondEnc.getValuesWriter(secondEncParams);

      return getFallbackValuesWriter(firstWriter, secondWriter);
    } else {
      return firstWriter;
    }
  }

  private <I extends ValuesWriter & RequiresFallback> ValuesWriter getFallbackValuesWriter(ValuesWriter initialWriter, ValuesWriter fallbackWriter) {
    if ( ! (initialWriter instanceof RequiresFallback) ) {
      throw new IllegalArgumentException("Initial writer must be subType of RequiresFallback. It is: " + initialWriter.getClass());
    }

    return FallbackValuesWriter.of(
      (I)initialWriter,
      fallbackWriter);
  }

  private ValuesWriter getBooleanValuesWriter(ColumnDescriptor path) {
    // no dictionary encoding for boolean
    if (writerVersion == WriterVersion.PARQUET_1_0) {
      return new BooleanPlainValuesWriter();
    } else {
      return new RunLengthBitPackingHybridValuesWriter(1, initialSlabSize, pageSizeThreshold, allocator);
    }
  }

  private ValuesWriter getFixedLenByteArrayValuesWriter(ColumnDescriptor path) {
    if (writerVersion == WriterVersion.PARQUET_1_0) {
      // dictionary encoding was not enabled in PARQUET 1.0
      return new FixedLenByteArrayPlainValuesWriter(path.getTypeLength(), initialSlabSize, pageSizeThreshold, allocator);
    } else {
      ValuesWriter fallbackWriter = new DeltaByteArrayWriter(initialSlabSize, pageSizeThreshold, allocator);
      return dictWriterWithFallBack(path, fallbackWriter);
    }
  }

  private ValuesWriter getBinaryValuesWriter(ColumnDescriptor path) {
    if (writerVersion == WriterVersion.PARQUET_1_0) {
      ValuesWriter fallbackWriter = new PlainValuesWriter(initialSlabSize, pageSizeThreshold, allocator);
      return dictWriterWithFallBack(path, fallbackWriter);
    } else {
      ValuesWriter fallbackWriter = new DeltaByteArrayWriter(initialSlabSize, pageSizeThreshold, allocator);
      return dictWriterWithFallBack(path, fallbackWriter);
    }
  }

  private ValuesWriter getInt32ValuesWriter(ColumnDescriptor path) {
    if (writerVersion == WriterVersion.PARQUET_1_0) {
      ValuesWriter fallbackWriter = new PlainValuesWriter(initialSlabSize, pageSizeThreshold, allocator);
      return dictWriterWithFallBack(path, fallbackWriter);
    } else {
      ValuesWriter fallbackWriter = new DeltaBinaryPackingValuesWriterForInteger(initialSlabSize, pageSizeThreshold, allocator);
      return dictWriterWithFallBack(path, fallbackWriter);
    }
  }

  private ValuesWriter getInt64ValuesWriter(ColumnDescriptor path) {
    if (writerVersion == WriterVersion.PARQUET_1_0) {
      ValuesWriter fallbackWriter = new PlainValuesWriter(initialSlabSize, pageSizeThreshold, allocator);
      return dictWriterWithFallBack(path, fallbackWriter);
    } else {
      ValuesWriter fallbackWriter = new DeltaBinaryPackingValuesWriterForLong(initialSlabSize, pageSizeThreshold, allocator);
      return dictWriterWithFallBack(path, fallbackWriter);
    }
  }

  private ValuesWriter getInt96ValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new FixedLenByteArrayPlainValuesWriter(12, initialSlabSize, pageSizeThreshold, allocator);
    return dictWriterWithFallBack(path, fallbackWriter);
  }

  private ValuesWriter getDoubleValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new PlainValuesWriter(initialSlabSize, pageSizeThreshold, allocator);
    return dictWriterWithFallBack(path, fallbackWriter);
  }

  private ValuesWriter getFloatValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new PlainValuesWriter(initialSlabSize, pageSizeThreshold, allocator);
    return dictWriterWithFallBack(path, fallbackWriter);
  }

  @SuppressWarnings("deprecation")
  private DictionaryValuesWriter dictionaryWriter(ColumnDescriptor path) {
    Encoding encodingForDataPage;
    Encoding encodingForDictionaryPage;
    switch(writerVersion) {
      case PARQUET_1_0:
        encodingForDataPage = PLAIN_DICTIONARY;
        encodingForDictionaryPage = PLAIN_DICTIONARY;
        break;
      case PARQUET_2_0:
        encodingForDataPage = RLE_DICTIONARY;
        encodingForDictionaryPage = PLAIN;
        break;
      default:
        throw new IllegalArgumentException("Unknown version: " + writerVersion);
    }
    switch (path.getType()) {
      case BOOLEAN:
        throw new IllegalArgumentException("no dictionary encoding for BOOLEAN");
      case BINARY:
        return new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, allocator);
      case INT32:
        return new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, allocator);
      case INT64:
        return new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, allocator);
      case INT96:
        return new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(dictionaryPageSizeThreshold, 12, encodingForDataPage, encodingForDictionaryPage, allocator);
      case DOUBLE:
        return new DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, allocator);
      case FLOAT:
        return new DictionaryValuesWriter.PlainFloatDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, allocator);
      case FIXED_LEN_BYTE_ARRAY:
        return new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(dictionaryPageSizeThreshold, path.getTypeLength(), encodingForDataPage, encodingForDictionaryPage, allocator);
      default:
        throw new IllegalArgumentException("Unknown type " + path.getType());
    }
  }

  private ValuesWriter dictWriterWithFallBack(ColumnDescriptor path, ValuesWriter writerToFallBackTo) {
    if (enableDictionary) {
      return FallbackValuesWriter.of(
        dictionaryWriter(path),
        writerToFallBackTo);
    } else {
      return writerToFallBackTo;
    }
  }
}
