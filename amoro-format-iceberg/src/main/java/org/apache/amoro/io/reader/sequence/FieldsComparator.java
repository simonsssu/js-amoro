package org.apache.amoro.io.reader.sequence;

import java.util.Comparator;

public interface FieldsComparator<T> extends Comparator<T> {
  int[] compareFields();
}
