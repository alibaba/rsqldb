 /*
  * Copyright 1999-2023 Alibaba Group Holding Ltd.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *      http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.alibaba.rsqldb.parser;

public interface CreateAndInsertSQL {
    String createTable = "create table odeum(`id` INT,`name` VARCHAR, `gmt_modified` TIMESTAMP) WITH (topic = 'rsqldb-odeum', data_format='JSON');";

    String createProcessTimeTable = "create table odeum(" +
                "`id` INT," +
                "`name` VARCHAR, " +
                "`time` as PROCTIME()" +
            ") WITH (" +
                "topic = 'rsqldb-odeum', " +
                "data_format='JSON'" +
            ");";

    String createViewJoin = "CREATE VIEW result_view AS\n" +
            "SELECT\n" +
            "    a.ticket_id     AS ticket_id,\n" +
            "    a.`position`    AS `position`,\n" +
            "    b.name          AS odeum_name,\n" +
            "    a.perform_name  AS perform_name\n" +
            "FROM test_view as a JOIN odeum AS b ON a.odeum_id = b.id;\n";

    String createViewWindow = "CREATE VIEW test_view AS\n" +
            "SELECT\n" +
            "    TUMBLE_START(ts, INTERVAL '10' MINUTE)     as window_start,\n" +
            "    username                                as username,\n" +
            "    count(click_url)                        as clicks\n" +
            "FROM user_clicks\n" +
            "GROUP BY TUMBLE(ts, INTERVAL '10' MINUTE), username;";

    String createViewQuery = "CREATE VIEW test_view AS\n" +
            "SELECT\n" +
            "    age                                     as age,\n" +
            "    username                                as username,\n" +
            "    count(click_url)                        as clicks\n" +
            "FROM user_clicks;";

    String createViewFilterQuery = "CREATE VIEW test_view AS\n" +
            "SELECT\n" +
            "    age                                     as age,\n" +
            "    username                                as username,\n" +
            "    count(click_url)                        as clicks\n" +
            "FROM user_clicks\n" +
            "WHERE age = 10";
    String createViewJointGroupByHaving = "CREATE VIEW test_view AS " +
            " SELECT Websites.name as name, Websites.url as url, SUM(access_log.count) AS nums FROM access_log " +
            " INNER JOIN Websites ON access_log.site_id=Websites.id\n" +
            " GROUP BY Websites.name\n" +
            " HAVING SUM(access_log.count) > 200;";

    String createViewJointWhereGBHavingStatement = "CREATE VIEW viewName AS " +
            " SELECT Websites.name as name, Websites.url as url, SUM(access_log.count) AS nums FROM access_log " +
            " WHERE Websites.url='test'" +
            " INNER JOIN Websites ON Websites.id = access_log.site_id\n" +
            " WHERE Websites.name='testName'" +
            " GROUP BY Websites.name\n" +
            " HAVING SUM(access_log.count) > 200;";

    String insertValue = "INSERT INTO `purchaser_dim` VALUES (1,'tom','male','16');";

    String insertValueField = "INSERT INTO Customers (CustomerName VARCHAR, ContactName VARCHAR, Address VARCHAR, City VARCHAR, PostalCode INT, Country VARCHAR)\n" +
            "VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');";

    String insertSelect = "insert into test_sink\n" +
            "select field_1\n" +
            "     , field_2\n" +
            "     , field_3\n" +
            "     , field_4\n" +
            "from test_source where field_1='1';";

    String insertSelectField = "INSERT INTO Customers (CustomerName, ContactName, Address, City)\n" +
            "select field_1\n" +
            "     , field_2\n" +
            "     , field_3\n" +
            "     , field_4\n" +
            "from test_source where field_1='1'";


}
