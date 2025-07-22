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

package org.apache.amoro.flink.table;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.STRING;

import org.apache.amoro.BasicTableTestHelper;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableTestHelper;
import org.apache.amoro.catalog.BasicCatalogTestHelper;
import org.apache.amoro.catalog.CatalogTestHelper;
import org.apache.amoro.flink.FlinkTestBase;
import org.apache.amoro.flink.util.DataUtil;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestLocal extends FlinkTestBase {

  public TestLocal(
      CatalogTestHelper catalogTestHelper, TableTestHelper tableTestHelper, boolean isHive) {
    super(catalogTestHelper, tableTestHelper);
  }

  @Parameterized.Parameters(name = "{0}, {1}, {2}")
  public static Collection parameters() {
    return Arrays.asList(
        new Object[][] {
          {
            new BasicCatalogTestHelper(TableFormat.MIXED_ICEBERG),
            new BasicTableTestHelper(true, true),
            false
          }
        });
  }

  @Test
  public void testAMSLocalPartialUpdate() {

    List<Object[]> data = new LinkedList<>();
    data.add(new Object[] {1001, Map.of("x", "42", "y", "78"), "2025-07-30", "02"});
    data.add(new Object[] {1001, Map.of("foo", "91", "bar", "15"), "2025-07-30", "02"});
    data.add(new Object[] {1001, Map.of("temp", "63", "hum", "30"), "2025-07-30", "02"});
    data.add(new Object[] {1002, Map.of("alpha", "7", "beta", "88"), "2025-07-30", "02"});
    data.add(new Object[] {1002, Map.of("k1", "55", "k2", "19"), "2025-07-30", "02"});
    data.add(new Object[] {1002, Map.of("x", "42", "y", "78"), "2025-07-30", "02"});
    data.add(new Object[] {1003, Map.of("foo", "91", "bar", "15"), "2025-07-30", "02"});
    data.add(new Object[] {1003, Map.of("temp", "63", "hum", "30"), "2025-07-30", "02"});
    data.add(new Object[] {1004, Map.of("alpha", "7", "beta", "88"), "2025-07-30", "02"});
    data.add(new Object[] {1004, Map.of("k1", "55", "k2", "19"), "2025-07-30", "02"});

    List<ApiExpression> rows = DataUtil.toRows(data);

    Table input =
        getTableEnv()
            .fromValues(
                DataTypes.ROW(
                    FIELD("item_id", BIGINT()),
                    FIELD("basic_map", MAP(STRING(), STRING())),
                    FIELD("dt", STRING()),
                    FIELD("hr", STRING())),
                rows);

    getTableEnv().createTemporaryView("input", input);

    sql(
        "CREATE CATALOG mixed_catalog WITH (\n"
            + "\t'type' = 'mixed_iceberg',\n"
            + "    'catalog-type' = 'custom',\n"
            + "    'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',\n"
            + "    'uri' = 'http://localhost:1630/api/iceberg/rest',\n"
            + "    'warehouse' = 't1'\n"
            + ");");

    final String dbname = "db_name1";
    final String tbname = "partial_sinkt2";

    sql(
        "insert into mixed_catalog."
            + dbname
            + "."
            + tbname
            + " select item_id, basic_map, dt, hr from input");

    List<Row> actual =
        sql(
            "select * from mixed_catalog."
                + dbname
                + "."
                + tbname
                + "/*+ OPTIONS('streaming'='false', 'scan.startup.mode'='earliest')*/");

    actual.stream()
        .forEach(
            f -> {
              System.out.println("--> " + f);
            });
  }
}
