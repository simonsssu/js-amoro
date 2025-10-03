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

package org.apache.amoro.io.reader.sequence;

import java.util.concurrent.Callable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.BiFunction;

// Compare value in seq group. e.g. fields.col1,col2.sequence-fields. it will compare values of
// col1 and col2, if values of col1 are equal, then compare col2.
public abstract class ValuesComparator<T> implements Comparator<T> {

    protected int seqFieldIndex;
    protected Type fieldType;

    public ValuesComparator(int seqFieldIndex, Type fieldType) {
      this.seqFieldIndex = seqFieldIndex;
      this.fieldType = fieldType;
    }

  // By default, null is the smallest. so if o2 is null, then return 1 (o1 > o2)
  // which means we should not update if current sequence field is null.
  public <V> int compare(V o1, V o2, BiFunction<V, V, Integer> func) {
    if (Objects.isNull(o1) && Objects.isNull(o2)) {
      return 0;
    } else if (Objects.isNull(o1)) {
      return -1;
    } else if (Objects.isNull(o2)) {
      return 1;
    } else {
      return func.apply(o1, o2);
    }
  }

}
