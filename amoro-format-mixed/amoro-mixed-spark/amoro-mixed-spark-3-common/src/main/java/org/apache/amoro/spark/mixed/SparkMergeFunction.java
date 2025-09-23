/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.spark.mixed;

import org.apache.amoro.io.reader.AbstractMergeFunction;
import org.apache.amoro.io.reader.sequence.FieldsComparator;
import org.apache.amoro.io.reader.sequence.ValuesComparator;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.PrimaryKeySpec;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SparkMergeFunction extends AbstractMergeFunction<InternalRow> {

  private final List<DataType> dataTypeList;

  public SparkMergeFunction(
      Types.StructType struct, PrimaryKeySpec primaryKeySpec, Map<String, String> properties) {
    super(
        struct,
        primaryKeySpec,
        properties,
        SparkConvertFromFunction.get(),
        SparkConvertToFunction.get());
    dataTypeList =
        Arrays.stream(SparkSchemaUtil.convert(struct.asSchema()).fields())
            .map(StructField::dataType)
            .collect(Collectors.toList());
  }

  @Override
  public InternalRow merge(InternalRow record, InternalRow update) {
    return new UpdatedRow(record, update);
  }

  @Override
  protected Supplier<FieldsComparator<InternalRow>> getFieldsComparator(StructType struct, int[] sequenceFields) {
    return () -> SparkSequenceFieldsComparator.create(struct, sequenceFields);
  }

  class UpdatedRow extends InternalRow {

    private final InternalRow originalRow;
    private final InternalRow updateRow;

    private UpdatedRow(InternalRow originalRow, InternalRow updateRow) {
      this.originalRow = originalRow;
      this.updateRow = updateRow;
    }

    @Override
    public int numFields() {
      return originalRow.numFields();
    }

    @Override
    public void setNullAt(int i) {
      originalRow.setNullAt(i);
      updateRow.setNullAt(i);
    }

    @Override
    public void update(int i, Object value) {
      updateRow.update(i, value);
    }

    @Override
    public InternalRow copy() {
      return new UpdatedRow(originalRow, updateRow);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return getObject(ordinal) == null;
    }

    @Override
    public boolean getBoolean(int ordinal) {
      return (boolean) getObject(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
      return (byte) getObject(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
      return (short) getObject(ordinal);
    }

    @Override
    public int getInt(int ordinal) {
      return (int) getObject(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
      return (long) getObject(ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
      return (float) getObject(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
      return (double) getObject(ordinal);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      return (Decimal) getObject(ordinal);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      return (UTF8String) getObject(ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return (byte[]) getObject(ordinal);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      return (CalendarInterval) getObject(ordinal);
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return (InternalRow) getObject(ordinal);
    }

    @Override
    public ArrayData getArray(int ordinal) {
      return (ArrayData) getObject(ordinal);
    }

    @Override
    public MapData getMap(int ordinal) {
      return (MapData) getObject(ordinal);
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      return getObject(ordinal, dataType);
    }

    private Object getObject(int ordinal) {
      return getObject(ordinal, dataTypeList.get(ordinal));
    }

    private Object getObject(int ordinal, DataType type) {
      Supplier<FieldsComparator<InternalRow>> fieldsComparator = fieldToSeqComparator.get(ordinal);
      if (ordinal < originalRow.numFields() && ordinal < updateRow.numFields()) {
        if (Objects.isNull(fieldsComparator)) {
          return fieldMergeOperators[ordinal].apply(
              originalRow.get(ordinal, type), updateRow.get(ordinal, type));
        } else {
          // compare sequence field. if original is small then update with updateRow.
          // otherwise keep originalRow.
          if (fieldsComparator.get().compare(originalRow, updateRow) <= 0) {
            return fieldMergeOperators[ordinal].apply(
                originalRow.get(ordinal, type), updateRow.get(ordinal, type));
          } else {
            return fieldMergeOperators[ordinal].apply(
                updateRow.get(ordinal, type), originalRow.get(ordinal, type));
          }
        }
      } else if (ordinal < originalRow.numFields()) {
        return originalRow.get(ordinal, type);
      } else {
        return updateRow.get(ordinal, type);
      }
    }
  }

  public static class SparkConvertFromFunction implements BiFunction<Type, Object, Object> {

    private static final SparkConvertFromFunction INSTANCE = new SparkConvertFromFunction();

    static SparkConvertFromFunction get() {
      return INSTANCE;
    }

    @Override
    public Object apply(Type type, Object o) {
      switch (type.typeId()) {
        case STRING:
          return ((UTF8String) o).toString();
        case DECIMAL:
          return ((Decimal) o).toJavaBigDecimal();
        case BINARY:
        case FIXED:
          return ByteBuffer.wrap((byte[]) o);
        case MAP:
          MapData mapData = (MapData) o;
          int size = mapData.numElements();
          Object[] keyArray = mapData.keyArray().array();
          Object[] valueArray = mapData.valueArray().array();
          Map<Object, Object> map = Maps.newHashMapWithExpectedSize(size);
          for (int i = 0; i < mapData.numElements(); i++) {
            map.put(keyArray[i], valueArray[i]);
          }
          return map;
        default:
          return o;
      }
    }
  }

  public static class SparkConvertToFunction implements BiFunction<Type, Object, Object> {

    private static final SparkConvertToFunction INSTANCE = new SparkConvertToFunction();

    static SparkConvertToFunction get() {
      return INSTANCE;
    }

    @Override
    public Object apply(Type type, Object o) {
      switch (type.typeId()) {
        case STRING:
          return UTF8String.fromString((String) o);
        case DECIMAL:
          return Decimal.fromDecimal(o);
        case BINARY:
        case FIXED:
          return ((ByteBuffer) o).array();
        case MAP:
          Map<Object, Object> map = (Map<Object, Object>) o;
          int size = map.size();
          List<Object> keyList = Lists.newArrayListWithCapacity(size);
          List<Object> valueList = Lists.newArrayListWithCapacity(size);
          map.forEach(
              (k, v) -> {
                keyList.add(k);
                valueList.add(v);
              });
          return new ArrayBasedMapData(
              new GenericArrayData(keyList.toArray()), new GenericArrayData(valueList.toArray()));
        default:
          return o;
      }
    }
  }

  public static class SparkSequenceFieldsComparator implements FieldsComparator<InternalRow> {

    private final int[] sequenceFields;

    private final StructType struct;

    private List<ValuesComparator<InternalRow>> comparators = Lists.newArrayList();

    private SparkSequenceFieldsComparator(StructType struct, int[] sequenceFields) {
      this.sequenceFields = sequenceFields;
      this.struct = struct;
      createValueComparator(struct, sequenceFields);
    }

    public static SparkSequenceFieldsComparator create(StructType struct, int[] sequenceFields) {
      if (sequenceFields.length == 0) {
        return null;
      }
      return new SparkSequenceFieldsComparator(struct, sequenceFields);
    }

    @Override
    public int[] compareFields() {
      return sequenceFields;
    }

    @Override
    public int compare(InternalRow o1, InternalRow o2) {
      for (ValuesComparator<InternalRow> comparator: comparators) {
        int res = comparator.compare(o1, o2);
        if (res == 0) continue;
        return res;
      }
      return 0;
    }

    private void createValueComparator(StructType struct, int[] sequenceFields) {
      for (int seqIndex: sequenceFields) {
        NestedField seqField = struct.field(seqIndex);
        comparators.add(SparkValuesComparator.create(seqField.fieldId(), seqField.type()));
      }
    }
  }

  public static class SparkValuesComparator extends ValuesComparator<InternalRow> {

    public static ValuesComparator<InternalRow> create(int seqFieldIndex, Type fieldType) {
      return new SparkValuesComparator(seqFieldIndex, fieldType);
    }

    private SparkValuesComparator(int seqFieldIndex, Type fieldType) {
      super(seqFieldIndex, fieldType);
    }

    @Override
    public int compare(InternalRow o1, InternalRow o2) {
      // Iceberg start from 1 while InternalRow start from 0.
      int fieldIndex = seqFieldIndex - 1;
      switch (fieldType.typeId()) {
        case BOOLEAN:
          return compare(o1.getBoolean(fieldIndex), o2.getBoolean(fieldIndex), Boolean::compareTo);
        case INTEGER:
        case DATE:
          return compare(o1.getInt(fieldIndex), o2.getInt(fieldIndex), Integer::compareTo);
        case LONG:
        case TIME:
        case TIMESTAMP:
          return compare(o1.getLong(fieldIndex), o2.getLong(fieldIndex), Long::compareTo);
        case FLOAT:
          return compare(o1.getFloat(fieldIndex), o2.getFloat(fieldIndex), Float::compareTo);
        case DOUBLE:
          return compare(o1.getDouble(fieldIndex), o2.getDouble(fieldIndex), Double::compareTo);
        case STRING:
          return compare(o1.getUTF8String(fieldIndex), o2.getUTF8String(fieldIndex), UTF8String::compare);
        case BINARY:
        case FIXED:
          return compare(o1.getBinary(fieldIndex), o2.getBinary(fieldIndex), Arrays::compare);
        case DECIMAL:
          return compare(
              o1.getDecimal(fieldIndex, ((Types.DecimalType) fieldType).precision(), ((Types.DecimalType) fieldType).scale()),
              o2.getDecimal(fieldIndex, ((Types.DecimalType) fieldType).precision(), ((Types.DecimalType) fieldType).scale()),
              Decimal::compare);
        default:
          throw new UnsupportedOperationException(
              "Unsupported sequence field type:" + fieldType);
      }
    }
  }
}
