package org.apache.amoro.io.reader.sequence;

import com.google.common.collect.Maps;
import java.util.Comparator;
import java.util.Map;
import java.util.function.BiFunction;

// Compare value in seq group. e.g. fields.col1,col2.sequence-fields. it will compare values of
// col1 and col2, if values of col1 are equal, then compare col2.
public class ValuesComparators {

  private Map<Class<?>, ValueComparator<?>> COMPARATOR_MAP = Maps.newHashMap();

  public ValuesComparators() {
    COMPARATOR_MAP.put(Integer.class, new IntComparator());
  }

  public abstract class ValueComparator<T> implements Comparator<T> {
  }

  public class IntComparator extends ValueComparator<Integer> {
    @Override
    public int compare(Integer o1, Integer o2) {
      return Integer.compare(o1, o2);
    }
  }
//
//  public int compare(T o1, T o2) {
//
//  }


}
