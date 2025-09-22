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

import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

public interface FieldMergeOperator extends BiFunction<Object, Object, Object> {

  String LAST = "last";
  String LAST_NON_NULL = "last_non_null_value";
  String FIRST = "first";
  String FIRST_NON_NULL = "first_non_null_value";
  String SUM = "sum";
  String PRODUCT = "product";
  String MAX = "max";
  String MIN = "min";
  String BOOL_AND = "bool_and";
  String BOOL_OR = "bool_or";
  String MERGE_MAP = "merge_map";

  Set<Type.TypeID> NUMBER_TYPES =
      Sets.newHashSet(
          Type.TypeID.INTEGER,
          Type.TypeID.LONG,
          Type.TypeID.FLOAT,
          Type.TypeID.DOUBLE,
          Type.TypeID.DECIMAL);

  Set<Type.TypeID> COMPARABLE_TYPES =
      Sets.newHashSet(
          Type.TypeID.INTEGER,
          Type.TypeID.LONG,
          Type.TypeID.FLOAT,
          Type.TypeID.DOUBLE,
          Type.TypeID.DECIMAL,
          Type.TypeID.TIME,
          Type.TypeID.TIMESTAMP,
          Type.TypeID.DATE,
          Type.TypeID.STRING);

  Set<Type.TypeID> BOOL_TYPES = Sets.newHashSet(Type.TypeID.BOOLEAN);

  Set<Type.TypeID> MAP_TYPES = Sets.newHashSet(Type.TypeID.MAP);

  // provide a reversed apply for sequence Group, by default apply method
  default Object applyReversed(Object currentValue, Object inputValue) {
    return apply(inputValue, currentValue);
  }

  abstract class AbstractFieldMergeOperator implements FieldMergeOperator {
    protected final BiFunction<Type, Object, Object> convertFromFunction;
    protected final BiFunction<Type, Object, Object> convertToFunction;

    protected final Type type;

    AbstractFieldMergeOperator(
        Type type,
        BiFunction<Type, Object, Object> convertFromFunction,
        BiFunction<Type, Object, Object> convertToFunction) {
      Preconditions.checkArgument(
          supportedTypes().contains(type.typeId()),
          "Unsupported type %s for %s",
          type.typeId().name(),
          this.getClass().getName());
      this.type = type;
      this.convertFromFunction = convertFromFunction;
      this.convertToFunction = convertToFunction;
    }

    abstract Set<Type.TypeID> supportedTypes();
  }

  class Last implements FieldMergeOperator {

    static Last INSTANCE = new Last();

    static Last get() {
      return INSTANCE;
    }

    @Override
    public Object apply(Object o, Object o2) {
      return o2;
    }
  }

  class LastNonNull implements FieldMergeOperator {

    static LastNonNull INSTANCE = new LastNonNull();

    static LastNonNull get() {
      return INSTANCE;
    }

    @Override
    public Object apply(Object o, Object o2) {
      return o2 == null ? o : o2;
    }
  }

  class First implements FieldMergeOperator {

    static First INSTANCE = new First();

    static First get() {
      return INSTANCE;
    }

    @Override
    public Object apply(Object o, Object o2) {
      return o;
    }
  }

  class FirstNonNull implements FieldMergeOperator {

    static FirstNonNull INSTANCE = new FirstNonNull();

    static FirstNonNull get() {
      return INSTANCE;
    }

    @Override
    public Object apply(Object o, Object o2) {
      return o != null ? o : o2;
    }
  }

  class Sum extends AbstractFieldMergeOperator {

    Sum(
        Type type,
        BiFunction<Type, Object, Object> convertFromFunction,
        BiFunction<Type, Object, Object> convertToFunction) {
      super(type, convertFromFunction, convertToFunction);
    }

    @Override
    public Object apply(Object o, Object o2) {
      // Ignore null value
      if (o == null) {
        return o2;
      }
      if (o2 == null) {
        return o;
      }
      switch (type.typeId()) {
        case INTEGER:
          return convertToFunction.apply(
              type,
              Integer.sum(
                  (Integer) convertFromFunction.apply(type, o),
                  (Integer) convertFromFunction.apply(type, o2)));
        case LONG:
          return convertToFunction.apply(
              type,
              Long.sum(
                  (Long) convertFromFunction.apply(type, o),
                  (Long) convertFromFunction.apply(type, o2)));
        case FLOAT:
          return convertToFunction.apply(
              type,
              Float.sum(
                  (Float) convertFromFunction.apply(type, o),
                  (Float) convertFromFunction.apply(type, o2)));
        case DOUBLE:
          return convertToFunction.apply(
              type,
              Double.sum(
                  (Double) convertFromFunction.apply(type, o),
                  (Double) convertFromFunction.apply(type, o2)));
        case DECIMAL:
          return convertToFunction.apply(
              type,
              ((BigDecimal) convertFromFunction.apply(type, o))
                  .add((BigDecimal) convertFromFunction.apply(type, o2)));
        default:
          throw new RuntimeException("Unsupported type " + type.typeId().name());
      }
    }

    @Override
    Set<Type.TypeID> supportedTypes() {
      return NUMBER_TYPES;
    }
  }

  class Product extends AbstractFieldMergeOperator {

    Product(
        Type type,
        BiFunction<Type, Object, Object> convertFromFunction,
        BiFunction<Type, Object, Object> convertToFunction) {
      super(type, convertFromFunction, convertToFunction);
    }

    @Override
    public Object apply(Object o, Object o2) {
      // Ignore null value
      if (o == null) {
        return o2;
      }
      if (o2 == null) {
        return o;
      }
      switch (type.typeId()) {
        case INTEGER:
          return convertToFunction.apply(
              type,
              (Integer) convertFromFunction.apply(type, o)
                  * (Integer) convertFromFunction.apply(type, o2));
        case LONG:
          return convertToFunction.apply(
              type,
              (Long) convertFromFunction.apply(type, o)
                  * (Long) convertFromFunction.apply(type, o2));
        case FLOAT:
          return convertToFunction.apply(
              type,
              (Float) convertFromFunction.apply(type, o)
                  * (Float) convertFromFunction.apply(type, o2));
        case DOUBLE:
          return convertToFunction.apply(
              type,
              (Double) convertFromFunction.apply(type, o)
                  * (Double) convertFromFunction.apply(type, o2));
        case DECIMAL:
          return convertToFunction.apply(
              type,
              ((BigDecimal) convertFromFunction.apply(type, o))
                  .multiply((BigDecimal) convertFromFunction.apply(type, o2)));
        default:
          throw new RuntimeException("Unsupported type " + type.typeId().name());
      }
    }

    @Override
    Set<Type.TypeID> supportedTypes() {
      return NUMBER_TYPES;
    }
  }

  class Max extends AbstractFieldMergeOperator {

    Max(
        Type type,
        BiFunction<Type, Object, Object> convertFromFunction,
        BiFunction<Type, Object, Object> convertToFunction) {
      super(type, convertFromFunction, convertToFunction);
    }

    @Override
    public Object apply(Object o, Object o2) {
      // Ignore null value
      if (o == null) {
        return o2;
      }
      if (o2 == null) {
        return o;
      }
      switch (type.typeId()) {
        case DATE:
        case INTEGER:
          return convertToFunction.apply(
              type,
              Integer.max(
                  (Integer) convertFromFunction.apply(type, o),
                  (Integer) convertFromFunction.apply(type, o2)));
        case TIME:
        case TIMESTAMP:
        case LONG:
          return convertToFunction.apply(
              type,
              Long.max(
                  (Long) convertFromFunction.apply(type, o),
                  (Long) convertFromFunction.apply(type, o2)));
        case FLOAT:
          return convertToFunction.apply(
              type,
              Float.max(
                  (Float) convertFromFunction.apply(type, o),
                  (Float) convertFromFunction.apply(type, o2)));
        case DOUBLE:
          return convertToFunction.apply(
              type,
              Double.max(
                  (Double) convertFromFunction.apply(type, o),
                  (Double) convertFromFunction.apply(type, o2)));
        case DECIMAL:
          return convertToFunction.apply(
              type,
              ((BigDecimal) convertFromFunction.apply(type, o))
                  .max((BigDecimal) convertFromFunction.apply(type, o2)));
        case STRING:
          CharSequence cs1 = (CharSequence) convertFromFunction.apply(type, o);
          CharSequence cs2 = (CharSequence) convertFromFunction.apply(type, o2);
          return convertToFunction.apply(
              type, cs1.toString().compareTo(cs2.toString()) > 0 ? cs1 : cs2);
        default:
          throw new RuntimeException("Unsupported type " + type.typeId().name());
      }
    }

    @Override
    Set<Type.TypeID> supportedTypes() {
      return COMPARABLE_TYPES;
    }
  }

  class Min extends AbstractFieldMergeOperator {

    Min(
        Type type,
        BiFunction<Type, Object, Object> convertFromFunction,
        BiFunction<Type, Object, Object> convertToFunction) {
      super(type, convertFromFunction, convertToFunction);
    }

    @Override
    public Object apply(Object o, Object o2) {
      // Ignore null value
      if (o == null) {
        return o2;
      }
      if (o2 == null) {
        return o;
      }
      switch (type.typeId()) {
        case DATE:
        case INTEGER:
          return convertToFunction.apply(
              type,
              Integer.min(
                  (Integer) convertFromFunction.apply(type, o),
                  (Integer) convertFromFunction.apply(type, o2)));
        case TIME:
        case TIMESTAMP:
        case LONG:
          return convertToFunction.apply(
              type,
              Long.min(
                  (Long) convertFromFunction.apply(type, o),
                  (Long) convertFromFunction.apply(type, o2)));
        case FLOAT:
          return convertToFunction.apply(
              type,
              Float.min(
                  (Float) convertFromFunction.apply(type, o),
                  (Float) convertFromFunction.apply(type, o2)));
        case DOUBLE:
          return convertToFunction.apply(
              type,
              Double.min(
                  (Double) convertFromFunction.apply(type, o),
                  (Double) convertFromFunction.apply(type, o2)));
        case DECIMAL:
          return convertToFunction.apply(
              type,
              ((BigDecimal) convertFromFunction.apply(type, o))
                  .min((BigDecimal) convertFromFunction.apply(type, o2)));
        case STRING:
          CharSequence cs1 = (CharSequence) convertFromFunction.apply(type, o);
          CharSequence cs2 = (CharSequence) convertFromFunction.apply(type, o2);
          return convertToFunction.apply(
              type, cs1.toString().compareTo(cs2.toString()) < 0 ? cs1 : cs2);
        default:
          throw new RuntimeException("Unsupported type " + type.typeId().name());
      }
    }

    @Override
    Set<Type.TypeID> supportedTypes() {
      return COMPARABLE_TYPES;
    }
  }

  class BoolAnd extends AbstractFieldMergeOperator {

    BoolAnd(
        Type type,
        BiFunction<Type, Object, Object> convertFromFunction,
        BiFunction<Type, Object, Object> convertToFunction) {
      super(type, convertFromFunction, convertToFunction);
    }

    @Override
    public Object apply(Object o, Object o2) {
      // Ignore null value
      if (o == null) {
        return o2;
      }
      if (o2 == null) {
        return o;
      }
      if (Objects.requireNonNull(type.typeId()) == Type.TypeID.BOOLEAN) {
        return convertToFunction.apply(
            type,
            ((boolean) convertFromFunction.apply(type, o))
                && ((boolean) convertFromFunction.apply(type, o2)));
      }
      throw new RuntimeException("Unsupported type " + type.typeId().name());
    }

    @Override
    Set<Type.TypeID> supportedTypes() {
      return BOOL_TYPES;
    }
  }

  class BoolOr extends AbstractFieldMergeOperator {

    BoolOr(
        Type type,
        BiFunction<Type, Object, Object> convertFromFunction,
        BiFunction<Type, Object, Object> convertToFunction) {
      super(type, convertFromFunction, convertToFunction);
    }

    @Override
    public Object apply(Object o, Object o2) {
      // Ignore null value
      if (o == null) {
        return o2;
      }
      if (o2 == null) {
        return o;
      }
      if (Objects.requireNonNull(type.typeId()) == Type.TypeID.BOOLEAN) {
        return convertToFunction.apply(
            type,
            ((boolean) convertFromFunction.apply(type, o))
                || ((boolean) convertFromFunction.apply(type, o2)));
      }
      throw new RuntimeException("Unsupported type " + type.typeId().name());
    }

    @Override
    Set<Type.TypeID> supportedTypes() {
      return BOOL_TYPES;
    }
  }

  class MergeMap extends AbstractFieldMergeOperator {

    MergeMap(
        Type type,
        BiFunction<Type, Object, Object> convertFromFunction,
        BiFunction<Type, Object, Object> convertToFunction) {
      super(type, convertFromFunction, convertToFunction);
    }

    @Override
    public Object apply(Object o, Object o2) {
      // Ignore null value
      if (o == null) {
        return o2;
      }
      if (o2 == null) {
        return o;
      }
      if (Objects.requireNonNull(type.typeId()) == Type.TypeID.MAP) {
        Map<?, ?> map1 = (Map<?, ?>) convertFromFunction.apply(type, o);
        Map<?, ?> map2 = (Map<?, ?>) convertFromFunction.apply(type, o2);
        Map<Object, Object> resultMap = Maps.newHashMap(map1);
        resultMap.putAll(map2);
        return convertToFunction.apply(type, resultMap);
      }
      throw new RuntimeException("Unsupported type " + type.typeId().name());
    }

    @Override
    Set<Type.TypeID> supportedTypes() {
      return MAP_TYPES;
    }
  }
}
