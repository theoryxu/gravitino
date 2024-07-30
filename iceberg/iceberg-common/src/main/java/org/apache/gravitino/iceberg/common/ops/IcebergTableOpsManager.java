package org.apache.gravitino.iceberg.common.ops;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableOpsManager implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(IcebergTableOpsManager.class);

  public static final String DEFAULT_CATALOG = "default_catalog";

  private static final Splitter splitter = Splitter.on(",");

  private final Map<String, IcebergTableOps> icebergTableOpsMap;

  private final IcebergTableOpsProvider provider;

  public IcebergTableOpsManager(IcebergConfig config) {
    this.icebergTableOpsMap = Maps.newConcurrentMap();
    this.provider = createProvider(config);
    this.provider.initialize(config);
  }

  public IcebergTableOpsManager(IcebergTableOpsProvider provider) {
    this.icebergTableOpsMap = Maps.newConcurrentMap();
    this.provider = provider;
  }

  public IcebergTableOps getOps(String rawPrefix) {
    String prefix = shelling(rawPrefix);
    String cacheKey = prefix;
    if (StringUtils.isBlank(prefix)) {
      LOG.debug("prefix is empty, return default iceberg catalog");
      cacheKey = DEFAULT_CATALOG;
    }
    return icebergTableOpsMap.computeIfAbsent(cacheKey, k -> provider.getIcebergTableOps(prefix));
  }

  public Optional<String> getPrefix(String rawPrefix, String warehouse) {
    String prefix = shelling(rawPrefix);
    if (!StringUtils.isBlank(prefix)) {
      return Optional.of(prefix);
    } else {
      return provider.getPrefix(warehouse);
    }
  }

  private IcebergTableOpsProvider createProvider(IcebergConfig config) {
    try (IsolatedClassLoader isolatedClassLoader =
        IsolatedClassLoader.buildClassLoader(getClassPaths(config))) {
      return isolatedClassLoader.withClassLoader(
          cl -> {
            try {
              Class<?> providerClz =
                  cl.loadClass(config.get(IcebergConfig.ICEBERG_REST_SERVICE_CATALOG_PROVIDER));
              return (IcebergTableOpsProvider) providerClz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> getClassPaths(IcebergConfig config) {
    return splitter
        .trimResults()
        .omitEmptyStrings()
        .splitToStream(config.get(IcebergConfig.ICEBERG_REST_SERVICE_CATALOG_PROVIDER_CLASSPATH))
        .map(this::transferAbsolutePath)
        .collect(Collectors.toList());
  }

  private String transferAbsolutePath(String pathString) {
    Path path = Paths.get(pathString);
    if (Files.exists(path)) {
      return path.toAbsolutePath().toString();
    }

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    if (!path.isAbsolute() && gravitinoHome != null) {
      Path newPath = Paths.get(gravitinoHome, pathString);
      if (Files.exists(newPath)) {
        return newPath.toString();
      }
    }

    throw new RuntimeException(String.format("path %s don't exist", path.toAbsolutePath()));
  }

  private String shelling(String rawPrefix) {
    if (StringUtils.isBlank(rawPrefix)) {
      return rawPrefix;
    } else {
      return rawPrefix.replace("/", "");
    }
  }

  @Override
  public void close() throws Exception {
    for (String catalog : icebergTableOpsMap.keySet()) {
      icebergTableOpsMap.get(catalog).close();
    }
  }
}