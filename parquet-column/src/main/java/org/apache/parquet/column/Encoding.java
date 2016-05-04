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
package org.apache.parquet.column;

import static org.apache.parquet.column.values.bitpacking.Packer.BIG_ENDIAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;

import java.io.IOException;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesReader;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesWriter;
import org.apache.parquet.column.values.boundedint.ZeroIntegerValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainBinaryDictionary;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainDoubleDictionary;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainFloatDictionary;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainIntegerDictionary;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainLongDictionary;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesReader;
import org.apache.parquet.column.values.plain.BooleanPlainValuesReader;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesReader.DoublePlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.IntegerPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.ParquetEncodingException;

/**
 * encoding of the data
 *
 * @author Julien Le Dem
 *
 */
public enum Encoding {

  PLAIN {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      switch (descriptor.getType()) {
      case BOOLEAN:
        return new BooleanPlainValuesReader();
      case BINARY:
        return new BinaryPlainValuesReader();
      case FLOAT:
        return new FloatPlainValuesReader();
      case DOUBLE:
        return new DoublePlainValuesReader();
      case INT32:
        return new IntegerPlainValuesReader();
      case INT64:
        return new LongPlainValuesReader();
      case INT96:
        return new FixedLenByteArrayPlainValuesReader(12);
      case FIXED_LEN_BYTE_ARRAY:
        return new FixedLenByteArrayPlainValuesReader(descriptor.getTypeLength());
      default:
        throw new ParquetDecodingException("no plain reader for type " + descriptor.getType());
      }
    }

    @Override
    public ValuesWriter getValuesWriter(GetValuesWriterParams params) {
      validateWriterVersion(params.getWriterVersion());

      switch (params.getDescriptor().getType()) {
        case BOOLEAN:
          return new BooleanPlainValuesWriter();
        case BINARY:
        case FLOAT:
        case DOUBLE:
        case INT32:
        case INT64:
          return new PlainValuesWriter(params.getInitialCapacity(), params.getPageSize(), params.getAllocator());
        case INT96:
          return new FixedLenByteArrayPlainValuesWriter(params.getBitWidth(), params.getInitialCapacity(), params.getPageSize(), params.getAllocator());
        case FIXED_LEN_BYTE_ARRAY:
          return new FixedLenByteArrayPlainValuesWriter(params.getBitWidth(), params.getInitialCapacity(), params.getPageSize(), params.getAllocator());
        default:
          throw new ParquetDecodingException("no plain writer for type " + params.getDescriptor().getType());
      }
    }

    @Override
    public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage) throws IOException {
      switch (descriptor.getType()) {
      case BINARY:
        return new PlainBinaryDictionary(dictionaryPage);
      case FIXED_LEN_BYTE_ARRAY:
        return new PlainBinaryDictionary(dictionaryPage, descriptor.getTypeLength());
      case INT96:
        return new PlainBinaryDictionary(dictionaryPage, 12);
      case INT64:
        return new PlainLongDictionary(dictionaryPage);
      case DOUBLE:
        return new PlainDoubleDictionary(dictionaryPage);
      case INT32:
        return new PlainIntegerDictionary(dictionaryPage);
      case FLOAT:
        return new PlainFloatDictionary(dictionaryPage);
      default:
        throw new ParquetDecodingException("Dictionary encoding not supported for type: " + descriptor.getType());
      }
    }

  },

  /**
   * Actually a combination of bit packing and run length encoding.
   * TODO: Should we rename this to be more clear?
   */
  RLE {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      int bitWidth = BytesUtils.getWidthFromMaxInt(getMaxLevel(descriptor, valuesType));
      if(bitWidth == 0) {
        return new ZeroIntegerValuesReader();
      }
      return new RunLengthBitPackingHybridValuesReader(bitWidth);
    }

    @Override
    public ValuesWriter getValuesWriter(GetValuesWriterParams params) {
      validateWriterVersion(params.getWriterVersion());

      int bitWidth = BytesUtils.getWidthFromMaxInt(getMaxLevel(params.getDescriptor(), ValuesType.VALUES));
      return new RunLengthBitPackingHybridValuesWriter(bitWidth, params.getInitialCapacity(), params.getPageSize(), params.getAllocator());
    }

    @Override
    public WriterVersion minimumWriterVersion() {
      return WriterVersion.PARQUET_2_0;
    }
  },

  /**
   * @deprecated This is no longer used, and has been replaced by {@link #RLE}
   * which is combination of bit packing and rle
   */
  @Deprecated
  BIT_PACKED {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      return new ByteBitPackingValuesReader(getMaxLevel(descriptor, valuesType), BIG_ENDIAN);
    }

    @Override
    public ValuesWriter getValuesWriter(GetValuesWriterParams params) {
      validateWriterVersion(params.getWriterVersion());

      return new ByteBitPackingValuesWriter(getMaxLevel(params.getDescriptor(), ValuesType.VALUES), BIG_ENDIAN);
    }
  },

  /**
   * @deprecated now replaced by RLE_DICTIONARY for the data page encoding and PLAIN for the dictionary page encoding
   */
  @Deprecated
  PLAIN_DICTIONARY {
    @Override
    public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary) {
      return RLE_DICTIONARY.getDictionaryBasedValuesReader(descriptor, valuesType, dictionary);
    }

    @Override
    public ValuesWriter getValuesWriter(GetValuesWriterParams params) {
      validateWriterVersion(params.getWriterVersion());

      Encoding encodingForDataPage = PLAIN_DICTIONARY;
      Encoding encodingForDictionaryPage = PLAIN;
      return getDictionaryBasedValuesWriter(params, encodingForDataPage, encodingForDictionaryPage);
    }

    @Override
    public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage) throws IOException {
      return PLAIN.initDictionary(descriptor, dictionaryPage);
    }

    @Override
    public boolean usesDictionary() {
      return true;
    }

  },

  /**
   * Delta encoding for integers. This can be used for int columns and works best
   * on sorted data
   */
  DELTA_BINARY_PACKED {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      if(descriptor.getType() != INT32 && descriptor.getType() != INT64) {
        throw new ParquetDecodingException("Encoding DELTA_BINARY_PACKED is only supported for type INT32 and INT64");
      }
      return new DeltaBinaryPackingValuesReader();
    }

    @Override
    public ValuesWriter getValuesWriter(GetValuesWriterParams params) {
      validateWriterVersion(params.getWriterVersion());

      switch(params.getDescriptor().getType()) {
        case INT32:
          return new DeltaBinaryPackingValuesWriterForInteger(params.getInitialCapacity(), params.getPageSize(), params.getAllocator());
        case INT64:
          return new DeltaBinaryPackingValuesWriterForLong(params.getInitialCapacity(), params.getPageSize(), params.getAllocator());
        default:
          throw new ParquetEncodingException("Encoding DELTA_BINARY_PACKED is only supported for type INT32 and INT64");
      }
    }

    @Override
    public WriterVersion minimumWriterVersion() {
      return WriterVersion.PARQUET_2_0;
    }
  },

  /**
   * Encoding for byte arrays to separate the length values and the data. The lengths
   * are encoded using DELTA_BINARY_PACKED
   */
  DELTA_LENGTH_BYTE_ARRAY {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor,
        ValuesType valuesType) {
      if (descriptor.getType() != BINARY) {
        throw new ParquetDecodingException("Encoding DELTA_LENGTH_BYTE_ARRAY is only supported for type BINARY");
      }
      return new DeltaLengthByteArrayValuesReader();
    }

    @Override
    public ValuesWriter getValuesWriter(GetValuesWriterParams params) {
      validateWriterVersion(params.getWriterVersion());

      if (params.getDescriptor().getType() != BINARY) {
        throw new ParquetEncodingException("Encoding DELTA_LENGTH_BYTE_ARRAY is only supported for type BINARY");
      }
      return new DeltaLengthByteArrayValuesWriter(params.getInitialCapacity(), params.getPageSize(), params.getAllocator());
    }
  },

  /**
   * Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED.
   * Suffixes are stored as delta length byte arrays.
   */
  DELTA_BYTE_ARRAY {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor,
        ValuesType valuesType) {
      if (descriptor.getType() != BINARY && descriptor.getType() != FIXED_LEN_BYTE_ARRAY) {
        throw new ParquetDecodingException("Encoding DELTA_BYTE_ARRAY is only supported for type BINARY and FIXED_LEN_BYTE_ARRAY");
      }
      return new DeltaByteArrayReader();
    }

    @Override
    public ValuesWriter getValuesWriter(GetValuesWriterParams params) {
      validateWriterVersion(params.getWriterVersion());

      if (params.getDescriptor().getType() != BINARY && params.getDescriptor().getType() != FIXED_LEN_BYTE_ARRAY) {
        throw new ParquetEncodingException("Encoding DELTA_BYTE_ARRAY is only supported for type BINARY and FIXED_LEN_BYTE_ARRAY");
      }
      return new DeltaByteArrayWriter(params.getInitialCapacity(), params.getPageSize(), params.getAllocator());
    }

    @Override
    public WriterVersion minimumWriterVersion() {
      return WriterVersion.PARQUET_2_0;
    }
  },

  /**
   * Dictionary encoding: the ids are encoded using the RLE encoding
   */
  RLE_DICTIONARY {
    @Override
    public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary) {
      switch (descriptor.getType()) {
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
      case INT96:
      case INT64:
      case DOUBLE:
      case INT32:
      case FLOAT:
        return new DictionaryValuesReader(dictionary);
      default:
        throw new ParquetDecodingException("Dictionary encoding not supported for type: " + descriptor.getType());
      }
    }

    @Override
    public ValuesWriter getValuesWriter(GetValuesWriterParams params) {
      validateWriterVersion(params.getWriterVersion());

      Encoding encodingForDataPage = RLE_DICTIONARY;
      Encoding encodingForDictionaryPage = PLAIN;
      return getDictionaryBasedValuesWriter(params, encodingForDataPage, encodingForDictionaryPage);
    }

    @Override
    public boolean usesDictionary() {
      return true;
    }

    @Override
    public WriterVersion minimumWriterVersion() {
      return WriterVersion.PARQUET_2_0;
    }
  };

  /**
   * Encapsulates parameters needed to create new ValuesWriter classes as part of the getValuesWriter
   * call.
   */
  public static class GetValuesWriterParams {
    public GetValuesWriterParams(ColumnDescriptor descriptor,
                                 WriterVersion writerVersion,
                                 int bitWidth,
                                 int initialCapacity,
                                 int pageSize,
                                 ByteBufferAllocator allocator,
                                 int maxDictionaryByteSize) {
      this.descriptor = descriptor;
      this.writerVersion = writerVersion;
      this.bitWidth = bitWidth;
      this.initialCapacity = initialCapacity;
      this.pageSize = pageSize;
      this.allocator = allocator;
      this.maxDictionaryByteSize = maxDictionaryByteSize;
    }

    private ColumnDescriptor descriptor;
    public ColumnDescriptor getDescriptor() { return descriptor; }

    private WriterVersion writerVersion;
    public WriterVersion getWriterVersion() { return writerVersion; }

    private int bitWidth;
    public int getBitWidth() { return bitWidth; }

    private int initialCapacity;
    public int getInitialCapacity() { return initialCapacity; }

    private int pageSize;
    public int getPageSize() { return pageSize; }

    private ByteBufferAllocator allocator;
    public ByteBufferAllocator getAllocator() { return allocator; }

    private int maxDictionaryByteSize;
    public int getMaxDictionaryByteSize() { return maxDictionaryByteSize; }
  }

  int getMaxLevel(ColumnDescriptor descriptor, ValuesType valuesType) {
    int maxLevel;
    switch (valuesType) {
    case REPETITION_LEVEL:
      maxLevel = descriptor.getMaxRepetitionLevel();
      break;
    case DEFINITION_LEVEL:
      maxLevel = descriptor.getMaxDefinitionLevel();
      break;
    case VALUES:
      if(descriptor.getType() == BOOLEAN) {
        maxLevel = 1;
        break;
      }
    default:
      throw new ParquetDecodingException("Unsupported encoding for values: " + this);
    }
    return maxLevel;
  }

  protected ValuesWriter getDictionaryBasedValuesWriter(GetValuesWriterParams params, Encoding encodingForDataPage, Encoding encodingForDictionaryPage) {
    switch (params.getDescriptor().getType()) {
      case BOOLEAN:
        throw new ParquetEncodingException("no dictionary encoding for BOOLEAN");
      case BINARY:
        return new PlainBinaryDictionaryValuesWriter(params.getMaxDictionaryByteSize(), encodingForDataPage, encodingForDictionaryPage, params.getAllocator());
      case INT32:
        return new PlainIntegerDictionaryValuesWriter(params.getMaxDictionaryByteSize(), encodingForDataPage, encodingForDictionaryPage, params.getAllocator());
      case INT64:
        return new PlainLongDictionaryValuesWriter(params.getMaxDictionaryByteSize(), encodingForDataPage, encodingForDictionaryPage, params.getAllocator());
      case INT96:
        return new PlainFixedLenArrayDictionaryValuesWriter(params.getMaxDictionaryByteSize(), params.getBitWidth(), encodingForDataPage, encodingForDictionaryPage, params.getAllocator());
      case DOUBLE:
        return new PlainDoubleDictionaryValuesWriter(params.getMaxDictionaryByteSize(), encodingForDataPage, encodingForDictionaryPage, params.getAllocator());
      case FLOAT:
        return new PlainFloatDictionaryValuesWriter(params.getMaxDictionaryByteSize(), encodingForDataPage, encodingForDictionaryPage, params.getAllocator());
      case FIXED_LEN_BYTE_ARRAY:
        return new PlainFixedLenArrayDictionaryValuesWriter(params.getMaxDictionaryByteSize(), params.getBitWidth(), encodingForDataPage, encodingForDictionaryPage, params.getAllocator());
      default:
        throw new IllegalArgumentException("Unknown type " + params.getDescriptor().getType());
    }
  }

  /**
   * @return whether this encoding requires a dictionary
   */
  public boolean usesDictionary() {
    return false;
  }

  /**
   * initializes a dictionary from a page
   * @param dictionaryPage
   * @return the corresponding dictionary
   */
  public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage) throws IOException {
    throw new UnsupportedOperationException(this.name() + " does not support dictionary");
  }

  /**
   * To read decoded values that don't require a dictionary
   *
   * @param descriptor the column to read
   * @param valuesType the type of values
   * @return the proper values reader for the given column
   * @throw {@link UnsupportedOperationException} if the encoding is dictionary based
   */
  public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
    throw new UnsupportedOperationException("Error decoding " + descriptor + ". " + this.name() + " is dictionary based");
  }

  /**
   * To create appropriate values writers for for the given encoding + type.
   * @param params struct that encapsulates various params that needed to be passed to
   * create the ValueWriters
   * @return ValuesWriter for the given encoding + column type.
   */
  public ValuesWriter getValuesWriter(GetValuesWriterParams params) {
    throw new UnsupportedOperationException("Creation of values writer is not supported by: " + this.name());
  }

  /**
   * Returns the minimum writer version needed for this encoding.
   */
  public WriterVersion minimumWriterVersion() {
    return WriterVersion.PARQUET_1_0;
  }

  /**
   * Verify that the writer version is >= minimumWriterVersion for this encoding
   * @param version to test against
   */
  protected void validateWriterVersion(WriterVersion version) {
    // if writerVersion is 2.0, it will work with 1.0 encodings fine.
    // just need to check if we're trying to use a 2.0 encoding when version is 1.0
    if (version == WriterVersion.PARQUET_1_0 && version != minimumWriterVersion() ) {
      throw new ParquetEncodingException(
        "Unable to use encoding: " + this.name() +
        " with version: " + version + " it requires: " + minimumWriterVersion());
    }
  }

  /**
   * To read decoded values that require a dictionary
   *
   * @param descriptor the column to read
   * @param valuesType the type of values
   * @param dictionary the dictionary
   * @return the proper values reader for the given column
   * @throw {@link UnsupportedOperationException} if the encoding is not dictionary based
   */
  public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary) {
    throw new UnsupportedOperationException(this.name() + " is not dictionary based");
  }

}
