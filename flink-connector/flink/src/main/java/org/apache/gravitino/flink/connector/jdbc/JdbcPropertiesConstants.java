/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.flink.connector.jdbc;

import java.util.HashMap;
import java.util.Map;

public class JdbcPropertiesConstants {

  private JdbcPropertiesConstants() {}

  public static final String GRAVITINO_JDBC_USER = "jdbc-user";
  public static final String GRAVITINO_JDBC_PASSWORD = "jdbc-password";
  public static final String GRAVITINO_JDBC_URL = "jdbc-url";
  public static final String GRAVITINO_JDBC_DRIVER = "jdbc-driver";

  public static final String FLINK_JDBC_URL = "base-url";
  public static final String FLINK_JDBC_USER = "username";
  public static final String FLINK_JDBC_PASSWORD = "password";
  public static final String FLINK_DRIVER = "driver";
  public static final String FLINK_JDBC_DEFAULT_DATABASE = "default-database";

  public static final String FLINK_JDBC_TABLE_DATABASE_URL = "url";
  public static final String FLINK_JDBC_TABLE_NAME = "table-name";

  public static Map<String, String> flinkToGravitinoMap = new HashMap<>();
  public static Map<String, String> gravitinoToFlinkMap = new HashMap<>();

  static {
    flinkToGravitinoMap.put(FLINK_JDBC_USER, GRAVITINO_JDBC_USER);
    flinkToGravitinoMap.put(FLINK_JDBC_PASSWORD, GRAVITINO_JDBC_PASSWORD);
    flinkToGravitinoMap.put(FLINK_DRIVER, GRAVITINO_JDBC_DRIVER);
    flinkToGravitinoMap.put(FLINK_JDBC_URL, GRAVITINO_JDBC_URL);

    gravitinoToFlinkMap.put(GRAVITINO_JDBC_USER, FLINK_JDBC_USER);
    gravitinoToFlinkMap.put(GRAVITINO_JDBC_PASSWORD, FLINK_JDBC_PASSWORD);
  }
}
