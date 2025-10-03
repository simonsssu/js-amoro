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

package org.apache.amoro.io.reader;

import org.apache.amoro.io.reader.sequence.FieldsComparator;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.PrimaryKeySpec;
import org.apache.amoro.table.TableProperties;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.PropertyUtil;

import java.util.Arrays;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class AbstractMergeFunction<T> implements MergeFunction<T> {
  protected final String SEQUENCE_FIELDS = "sequence-fields";
  protected final String FIELD_PREFIX = "fields";

  protected final FieldMergeOperator[] fieldMergeOperators;

  protected final Map<Integer, Supplier<FieldsComparator<T>>> fieldToSeqComparator;

  public AbstractMergeFunction(
      Types.StructType struct,
      PrimaryKeySpec primaryKeySpec,
      Map<String, String> properties,
      BiFunction<Type, Object, Object> convertFromFunction,
      BiFunction<Type, Object, Object> convertToFunction) {
    this.fieldToSeqComparator = Maps.newHashMap();
    createSequenceFieldsComparator(struct, properties);

    String mergeFunction =
        PropertyUtil.propertyAsString(
            properties, TableProperties.MERGE_FUNCTION, TableProperties.MERGE_FUNCTION_DEFAULT);
    switch (mergeFunction) {
      case TableProperties.MERGE_FUNCTION_AGGREGATION:
        fieldMergeOperators =
            FieldMergeOperators.aggregationOperators(
                struct, primaryKeySpec, properties, convertFromFunction, convertToFunction);
        break;
      case TableProperties.MERGE_FUNCTION_PARTIAL_UPDATE:
        fieldMergeOperators = FieldMergeOperators.partialUpdateOperators(struct);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported merge function:" + mergeFunction);
    }
  }

  private void createSequenceFieldsComparator(
      Types.StructType struct, Map<String, String> properties) {
    properties.entrySet().stream()
        .filter(e -> e.getKey().startsWith(FIELD_PREFIX) && e.getKey().endsWith(SEQUENCE_FIELDS))
        .forEach(
            e -> {
              String sequenceFieldKey = e.getKey();
              int[] sequenceFields =
                  Arrays.stream(
                          sequenceFieldKey
                              .substring(
                                  FIELD_PREFIX.length() + 1,
                                  sequenceFieldKey.length() - SEQUENCE_FIELDS.length() - 1)
                              .split(","))
                      .mapToInt(fieldName -> struct.field(fieldName).fieldId())
                      .toArray();
              // Columns need to be updated.
              Supplier<FieldsComparator<T>> comparator =
                  getFieldsComparator(struct, sequenceFields);
              Stream.of(e.getValue().split(","))
                  .map(fieldName -> struct.field(fieldName).fieldId() - 1)
                  .forEach(
                      field -> {
                        if (fieldToSeqComparator.containsKey(field)) {
                          throw new IllegalArgumentException(
                              "Field "
                                  + field
                                  + " is already assigned to sequence fields."
                                  + struct.field(field));
                        }
                        // TODO put seq comparator here.
                        fieldToSeqComparator.put(field, comparator);
                      });
            });
  }

  protected abstract Supplier<FieldsComparator<T>> getFieldsComparator(
      StructType struct, int[] sequenceFields);
}
