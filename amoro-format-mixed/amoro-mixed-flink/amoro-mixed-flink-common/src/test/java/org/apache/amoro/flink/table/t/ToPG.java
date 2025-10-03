package org.apache.amoro.flink.table.t;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.amoro.flink.table.t.DataGenerator.TestData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import org.postgresql.util.HStoreConverter;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

public class ToPG {

  private static final String DB_URL = "jdbc:postgresql://localhost:5433/sisu";
  private static final String DB_USER = "sisu";
  private static final String DB_PASSWORD = "123456";
  private static final String TABLE_NAME = "items";

  public static void main(String[] args) throws JsonProcessingException {
    System.out.println("Generating 100,000 records...");
    List<TestData> dataList = DataGenerator.generate10WRecords();
    System.out.println("Data generation completed, total: " + dataList.size() + " records");

    writeToPostgreSQL(dataList);
    writeToKafaka(dataList);
  }

  private static void writeToKafaka(List<DataGenerator.TestData> dataList)
      throws JsonProcessingException {
    // Placeholder for Kafka writing logic
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    ObjectMapper om = new ObjectMapper();

      for (int i = 0; i < dataList.size(); i++) {
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("to2", om.writeValueAsString(dataList.get(i))));
      }
      producer.flush();
    System.out.println("Writing to Kafka is not implemented in this example.");
  }

  private static void writeToPostgreSQL(List<DataGenerator.TestData> dataList) {
    int batchSize = 1000;
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
      conn.setAutoCommit(false);

      String insertSql = String.format(
          "INSERT INTO %s (item_id, basic_map, dt, hr) " +
              "VALUES (?, ?::hstore, ?, ?) " +
              "ON CONFLICT (item_id, dt, hr) DO UPDATE " +
              "SET basic_map = %s.basic_map || excluded.basic_map",
          TABLE_NAME, TABLE_NAME
      );

      pstmt = conn.prepareStatement(insertSql);

      int count = 0;
      for (DataGenerator.TestData data : dataList) {
        pstmt.setLong(1, data.getItemId());

        Map<String, String> basicMap = data.getBasicMap();
        String hstoreValue = HStoreConverter.toString(basicMap);
        pstmt.setString(2, hstoreValue);

        pstmt.setString(3, data.getDt());
        pstmt.setString(4, data.getHr());

        pstmt.addBatch();
        count++;

        if (count % batchSize == 0) {
          pstmt.executeBatch();
          conn.commit();
          System.out.println("Inserted " + count + " records");
        }
      }

      if (count % batchSize != 0) {
        pstmt.executeBatch();
        conn.commit();
      }

      System.out.println("All data written, total inserted: " + count + " records");

    } catch (SQLException e) {
      System.err.println("Write failed: " + e.getMessage());
      if (conn != null) {
        try {
          conn.rollback();
          System.out.println("Transaction rolled back");
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
      }
    } finally {
      try {
        if (pstmt != null) pstmt.close();
        if (conn != null) conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}
