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
package org.apache.gravitino.iceberg.common.ops;

import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class TestGravitinoIcebergTableOpsProvider {
  private static final String STORE_PATH =
      "/tmp/gravitino_test_iceberg_jdbc_backend_" + UUID.randomUUID().toString().replace("-", "");

  @ParameterizedTest
  @ValueSource(strings = {"metalake.hive_backend", "metalake.jdbc_backend"})
  public void testValidIcebergTableOps(String prefix) {
    String[] segments = prefix.split("\\.");
    String metalakeName = segments[0];
    String catalogName = segments[1];

    Map<String, String> config = Maps.newHashMap();
    config.put(IcebergConstants.GRAVITINO_SERVER_URI, "http://127.0.0.1");
    GravitinoIcebergTableOpsProvider provider = new GravitinoIcebergTableOpsProvider();
    provider.initialize(config);

    Catalog catalog = Mockito.mock(Catalog.class);
    GravitinoMetalake gravitinoMetalake = Mockito.mock(GravitinoMetalake.class);
    GravitinoAdminClient gravitinoAdminClient = Mockito.mock(GravitinoAdminClient.class);

    Mockito.when(gravitinoAdminClient.loadMetalake(metalakeName)).thenReturn(gravitinoMetalake);
    Mockito.when(gravitinoMetalake.loadCatalog(catalogName)).thenReturn(catalog);
    Mockito.when(catalog.provider()).thenReturn("lakehouse-iceberg");
    if ("hive_backend".equals(catalogName)) {
      Mockito.when(catalog.properties())
          .thenReturn(
              new HashMap<String, String>() {
                {
                  put(IcebergConstants.CATALOG_BACKEND, "hive");
                  put(IcebergConstants.URI, "thrift://127.0.0.1:7004");
                  put(IcebergConstants.WAREHOUSE, "/tmp/usr/hive/warehouse");
                }
              });
    } else if ("jdbc_backend".equals(catalogName)) {
      Mockito.when(catalog.properties())
          .thenReturn(
              new HashMap<String, String>() {
                {
                  put(IcebergConstants.CATALOG_BACKEND, "jdbc");
                  put(
                      IcebergConstants.URI,
                      String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", STORE_PATH));
                  put(IcebergConstants.WAREHOUSE, "/tmp/user/hive/warehouse-jdbc");
                  put(IcebergConstants.GRAVITINO_JDBC_USER, "gravitino");
                  put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, "gravitino");
                  put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.h2.Driver");
                  put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true");
                }
              });
    }

    provider.setGravitinoClient(gravitinoAdminClient);
    IcebergTableOps ops = provider.getIcebergTableOps(prefix);

    if (StringUtils.isBlank(prefix)) {
      Assertions.assertEquals(ops.catalog.name(), "memory");
    } else {
      Assertions.assertEquals(ops.catalog.name(), catalogName);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "metalake", "metalake.hive_catalog"})
  public void testInvalidIcebergTableOps(String prefix) {
    Map<String, String> config = Maps.newHashMap();
    config.put(IcebergConstants.GRAVITINO_SERVER_URI, "http://127.0.0.1");
    GravitinoIcebergTableOpsProvider provider = new GravitinoIcebergTableOpsProvider();
    provider.initialize(config);

    Catalog catalog = Mockito.mock(Catalog.class);
    GravitinoMetalake gravitinoMetalake = Mockito.mock(GravitinoMetalake.class);
    GravitinoAdminClient gravitinoAdminClient = Mockito.mock(GravitinoAdminClient.class);

    Mockito.when(gravitinoAdminClient.loadMetalake("metalake")).thenReturn(gravitinoMetalake);
    Mockito.when(gravitinoMetalake.loadCatalog("hive_catalog")).thenReturn(catalog);
    Mockito.when(catalog.provider()).thenReturn("hive");

    provider.setGravitinoClient(gravitinoAdminClient);

    Assertions.assertThrowsExactly(
        RuntimeException.class, () -> provider.getIcebergTableOps(prefix));
  }
}
