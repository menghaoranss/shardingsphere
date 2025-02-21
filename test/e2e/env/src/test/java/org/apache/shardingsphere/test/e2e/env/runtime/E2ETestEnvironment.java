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

package org.apache.shardingsphere.test.e2e.env.runtime;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.sphereex.dbplusengine.SphereEx;
import lombok.Getter;
import org.apache.shardingsphere.test.e2e.env.runtime.cluster.ClusterEnvironment;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.path.ScenarioCommonPath;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * E2E test environment.
 */
@Getter
public final class E2ETestEnvironment {
    
    private static final E2ETestEnvironment INSTANCE = new E2ETestEnvironment();
    
    private final Collection<String> runModes;
    
    private final boolean runAdditionalTestCases;
    
    private final Collection<String> scenarios;
    
    @SphereEx
    private final String governanceCenter;
    
    private final ClusterEnvironment clusterEnvironment;
    
    private final boolean smoke;
    
    @SphereEx
    private final String nativeHost;
    
    @SphereEx
    private final String nativePort;
    
    @SphereEx
    private final String nativeUsername;
    
    @SphereEx
    private final String nativePassword;
    
    @SphereEx
    private final boolean externalEnable;
    
    @SphereEx
    private final String externalCaseRegex;
    
    @SphereEx
    private final Map<String, String> placeholderAndReplacementsMap;
    
    private E2ETestEnvironment() {
        Properties props = loadProperties();
        // SPEX CHANGED: BEGIN
        runModes = Splitter.on(",").trimResults().splitToList(props.getProperty("it.run.modes", ""));
        // SPEX CHANGED: END
        runAdditionalTestCases = Boolean.parseBoolean(props.getProperty("it.run.additional.cases"));
        TimeZone.setDefault(TimeZone.getTimeZone(props.getProperty("it.timezone", "UTC")));
        scenarios = getScenarios(props);
        smoke = Boolean.parseBoolean(props.getProperty("it.run.smoke"));
        // SPEX ADDED: BEGIN
        governanceCenter = Strings.isNullOrEmpty(props.getProperty("it.env.governance.center")) ? "ZooKeeper" : props.getProperty("it.env.governance.center");
        nativeHost = props.getProperty("it.native.host");
        nativePort = props.getProperty("it.native.port");
        nativeUsername = props.getProperty("it.native.username");
        nativePassword = props.getProperty("it.native.password");
        externalEnable = Boolean.parseBoolean(props.getProperty("it.external.enable"));
        externalCaseRegex = props.getProperty("it.external.case.regex", "");
        placeholderAndReplacementsMap = props.entrySet().stream().filter(entry -> entry.getKey().toString().startsWith("it.placeholder."))
                .collect(Collectors.toMap(entry -> "${" + entry.getKey().toString().substring(entry.getKey().toString().lastIndexOf(".") + 1) + "}", entry -> String.valueOf(entry.getValue())));
        // SPEX ADDED: END
        clusterEnvironment = new ClusterEnvironment(props);
    }
    
    @SuppressWarnings("AccessOfSystemProperties")
    private Properties loadProperties() {
        Properties result = new Properties();
        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("env/it-env.properties")) {
            result.load(inputStream);
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
        for (String each : System.getProperties().stringPropertyNames()) {
            result.setProperty(each, System.getProperty(each));
        }
        return result;
    }
    
    private Collection<String> getScenarios(final Properties props) {
        // SPEX CHANGED: BEGIN
        Collection<String> result = Splitter.on(",").trimResults().splitToList(props.getProperty("it.scenarios", ""));
        // SPEX CHANGED: END
        for (String each : result) {
            new ScenarioCommonPath(each).checkFolderExist();
        }
        return result;
    }
    
    /**
     * Get instance.
     *
     * @return singleton instance
     */
    public static E2ETestEnvironment getInstance() {
        return INSTANCE;
    }
}
