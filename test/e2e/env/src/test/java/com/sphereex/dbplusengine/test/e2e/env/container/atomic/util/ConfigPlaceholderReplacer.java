/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sphereex.dbplusengine.test.e2e.env.container.atomic.util;

import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.shardingsphere.infra.util.directory.ClasspathResourceDirectoryReader;
import org.apache.shardingsphere.test.e2e.env.runtime.E2ETestEnvironment;
import org.testcontainers.utility.Base58;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public final class ConfigPlaceholderReplacer {
    
    private static final String OS_MAC_TMP_DIR = "/tmp";
    
    private static final String E2E_PROXY_CONFIG_TMP_DIR_PREFIX = "test-e2e-config-";
    
    private static final String REMOVE_PLAIN_COLUMN_KEY = "${remove_plain_column}";
    
    private static final Pattern YAML_CONFIG_REMOVE_PLAIN_COLUMN_PATTERN = Pattern.compile("(?m)^(\\s*)plain:[\\r\\n]+(\\1\\s+.*[\\r\\n]+)*");
    
    /**
     * Get replaced resource.
     *
     * @param configResource config resource
     * @return replaced resource
     */
    public static String getReplacedResource(final String configResource) {
        return getReplacedResources(Collections.singleton(configResource)).values().iterator().next();
    }
    
    /**
     * Get replaced resources.
     *
     * @param configResources config resources
     * @return replaced resources
     */
    public static Map<String, String> getReplacedResources(final Collection<String> configResources) {
        return getReplacedResources(configResources, getPlaceholderAndReplacementsMap());
    }
    
    @SneakyThrows(IOException.class)
    private static Map<String, String> getReplacedResources(final Collection<String> configResources, final Map<String, String> placeholderAndReplacementsMap) {
        Map<String, String> result = new HashMap<>();
        Path tempDirectory = createTempDirectory().toPath();
        for (String each : configResources) {
            String configResource = StringUtils.removeStart(each, "/");
            if (ClasspathResourceDirectoryReader.isDirectory(configResource)) {
                Path subDirectory = tempDirectory.resolve(Paths.get(configResource).getFileName());
                Files.createDirectory(subDirectory);
                ClasspathResourceDirectoryReader.read(configResource).forEach(resource -> getReplacedTempFile(subDirectory, resource, placeholderAndReplacementsMap));
                result.put(each, subDirectory.toFile().getAbsolutePath());
            } else {
                result.put(each, getReplacedTempFile(tempDirectory, each, placeholderAndReplacementsMap).getAbsolutePath());
            }
        }
        return result;
    }
    
    @SneakyThrows(IOException.class)
    private static File getReplacedTempFile(final Path tempDirectory, final String configResource, final Map<String, String> placeholderAndReplacementsMap) {
        String content = getContent(configResource);
        content = isRemovePlainColumnEnabled(placeholderAndReplacementsMap) ? removePlainColumn(content, configResource) : replaceContent(content, placeholderAndReplacementsMap);
        File result = tempDirectory.resolve(new File(configResource).toPath().getFileName()).toFile();
        result.deleteOnExit();
        try (FileWriter writer = new FileWriter(result)) {
            writer.write(content);
        }
        return result;
    }
    
    private static String replaceContent(final String content, final Map<String, String> placeholderAndReplacementsMap) {
        String result = content;
        for (Entry<String, String> entry : placeholderAndReplacementsMap.entrySet()) {
            result = result.replace(entry.getKey(), entry.getValue());
        }
        return result;
    }
    
    private static boolean isRemovePlainColumnEnabled(final Map<String, String> placeholderAndReplacementsMap) {
        if (placeholderAndReplacementsMap.containsKey(REMOVE_PLAIN_COLUMN_KEY)) {
            return Boolean.parseBoolean(placeholderAndReplacementsMap.get(REMOVE_PLAIN_COLUMN_KEY));
        }
        return false;
    }
    
    private static String removePlainColumn(final String content, final String configResource) {
        if (configResource.endsWith(".yaml")) {
            return YAML_CONFIG_REMOVE_PLAIN_COLUMN_PATTERN.matcher(content).replaceAll("");
        }
        return content;
    }
    
    private static String getContent(final String configResource) throws IOException {
        String content;
        InputStream resourceAsStream = ConfigPlaceholderReplacer.class.getClassLoader().getResourceAsStream(StringUtils.removeStart(configResource, "/"));
        if (resourceAsStream != null) {
            content = IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8);
        } else {
            content = IOUtils.toString(new File(configResource).toURI(), StandardCharsets.UTF_8);
        }
        return content;
    }
    
    private static Map<String, String> getPlaceholderAndReplacementsMap() {
        return E2ETestEnvironment.getInstance().getPlaceholderAndReplacementsMap();
    }
    
    private static File createTempDirectory() {
        try {
            if (SystemUtils.IS_OS_MAC) {
                return Files.createTempDirectory(Paths.get(OS_MAC_TMP_DIR), E2E_PROXY_CONFIG_TMP_DIR_PREFIX).toFile();
            }
            return Files.createTempDirectory(E2E_PROXY_CONFIG_TMP_DIR_PREFIX).toFile();
        } catch (final IOException ex) {
            return new File(E2E_PROXY_CONFIG_TMP_DIR_PREFIX + Base58.randomString(5));
        }
    }
}
