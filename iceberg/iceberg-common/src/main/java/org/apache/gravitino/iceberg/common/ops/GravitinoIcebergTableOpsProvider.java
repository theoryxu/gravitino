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
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This provider proxy Gravitino lakehouse-iceberg catalogs.
 *
 * <p>For example, there are one catalog named iceberg_catalog in metalake
 *
 * <p>The prefix is like: metalake.iceberg_catalog
 */
public class GravitinoIcebergTableOpsProvider implements IcebergTableOpsProvider {
  public static final Logger LOG = LoggerFactory.getLogger(GravitinoIcebergTableOpsProvider.class);

  private static final String JDBC = "jdbc";
  private GravitinoAdminClient gravitinoClient;

  @Override
  public void initialize(Map<String, String> properties) {
    this.gravitinoClient =
        GravitinoAdminClient.builder(properties.get(IcebergConstants.GRAVITINO_SERVER_URI)).build();
  }

  @Override
  public IcebergTableOps getIcebergTableOps(String prefix) {
    if (StringUtils.isBlank(prefix)) {
      throw new RuntimeException("blank prefix is illegal");
    }

    String[] segments = prefix.split("\\.");
    if (segments.length != 2) {
      throw new RuntimeException(String.format("%s format is illegal", prefix));
    }

    String metalakeName = segments[0];
    String catalogName = segments[1];

    GravitinoMetalake metalake = gravitinoClient.loadMetalake(metalakeName);
    Catalog catalog = metalake.loadCatalog(catalogName);

    if (!"lakehouse-iceberg".equals(catalog.provider())) {
      throw new RuntimeException(
          String.format("%s.%s is not iceberg catalog", metalakeName, catalogName));
    }

    Map<String, String> properties = Maps.newHashMap(catalog.properties());
    properties.merge(
        IcebergConstants.CATALOG_BACKEND_NAME, catalogName, (oldValue, newValue) -> oldValue);
    if (JDBC.equals(properties.get(IcebergConstants.CATALOG_BACKEND))) {
      properties.put(
          IcebergConstants.ICEBERG_JDBC_PASSWORD,
          properties.get(IcebergConstants.GRAVITINO_JDBC_PASSWORD));
      properties.put(
          IcebergConstants.ICEBERG_JDBC_USER, properties.get(IcebergConstants.GRAVITINO_JDBC_USER));
    }
    return new IcebergTableOps(new IcebergConfig(properties));
  }

  public void setGravitinoClient(GravitinoAdminClient gravitinoClient) {
    this.gravitinoClient = gravitinoClient;
  }

  @Override
  public void close() throws Exception {
    this.gravitinoClient.close();
  }
}
