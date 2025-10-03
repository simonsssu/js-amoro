package org.apache.amoro.flink.table.t;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DataGenerator {



  public static List<TestData> generate10WRecords() {
    int totalRecords = 100000;
    int distinctItemIdsCnt = 5000;
    return generate(totalRecords, distinctItemIdsCnt);
  }

  public static List<TestData> generate(int totalRecords, int distinctItemIds) {
    List<TestData> dataList = new ArrayList<>(totalRecords);
    Random random = new Random();
    DateTimeFormatter dtFormatter = DateTimeFormatter.ISO_LOCAL_DATE;

    Map<Long, Set<String>> itemKeyCache = new HashMap<>(distinctItemIds);

    for (int i = 0; i < totalRecords; i++) {
      long itemId = random.nextInt(distinctItemIds) + 1;

      itemKeyCache.putIfAbsent(itemId, new HashSet<>());
      Set<String> usedKeys = itemKeyCache.get(itemId);

      Map<String, String> basicMap = new HashMap<>();
      int mapSize = random.nextInt(3) + 1;
      for (int j = 0; j < mapSize; j++) {
        String key;
        do {
          key = "key_" + itemId + "_" + random.nextInt(1000000);
        } while (usedKeys.contains(key));
        usedKeys.add(key);
        basicMap.put(key, "val_" + random.nextInt(1000));
      }

      LocalDate dt = LocalDate.now().minusDays(random.nextInt(1));
      String dtStr = dt.format(dtFormatter);

      String hr = String.format("%02d", random.nextInt(3)); // 00,01,02

      dataList.add(new TestData(itemId, basicMap, dtStr, hr));
    }

    return dataList;
  }

  public static class TestData {
    private final long itemId;
    private final Map<String, String> basicMap;
    private final String dt;
    private final String hr;

    public TestData(long itemId, Map<String, String> basicMap, String dt, String hr) {
      this.itemId = itemId;
      this.basicMap = basicMap;
      this.dt = dt;
      this.hr = hr;
    }

    public long getItemId() { return itemId; }
    public Map<String, String> getBasicMap() { return basicMap; }
    public String getDt() { return dt; }
    public String getHr() { return hr; }
  }
}