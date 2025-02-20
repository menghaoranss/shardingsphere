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

package com.sphereex.dbplusengine.test.e2e.engine.arg;

import com.sphereex.dbplusengine.test.e2e.env.runtime.scenario.path.ScenarioCasePath;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.stream.Streams;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.test.e2e.cases.casse.E2ETestCase;
import org.apache.shardingsphere.test.e2e.cases.casse.E2ETestCaseContext;
import org.apache.shardingsphere.test.e2e.env.runtime.E2ETestEnvironment;
import org.apache.shardingsphere.test.e2e.framework.param.model.CaseTestParameter;
import org.apache.shardingsphere.test.e2e.framework.type.SQLCommandType;
import org.h2.util.ScriptReader;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * E2E test case arguments provider.
 */
public final class BusinessScenarioTestCaseArgumentsProvider implements ArgumentsProvider {
    
    private static final E2ETestEnvironment ENV = E2ETestEnvironment.getInstance();
    
    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
        return Stream.concat(Stream.of(Arguments.of(new CaseTestParameter(null, null, null, null, null, null))),
                ENV.getScenarios().stream().flatMap(this::getTestCaseArguments));
    }
    
    private Stream<? extends Arguments> getTestCaseArguments(final String scenario) {
        return new ScenarioCasePath(scenario).getCasesPaths().stream().flatMap(each -> getTestCaseArguments(scenario, each));
    }
    
    private Stream<? extends Arguments> getTestCaseArguments(final String scenario, final String casesPath) {
        return ENV.getClusterEnvironment().getDatabaseTypes().stream().flatMap(each -> getTestCaseArguments(scenario, casesPath, each));
    }
    
    private Stream<? extends Arguments> getTestCaseArguments(final String scenario, final String casesPath, final DatabaseType databaseType) {
        return ENV.getRunModes().stream().flatMap(each -> getTestCaseArguments(scenario, casesPath, databaseType, each));
    }
    
    private Stream<? extends Arguments> getTestCaseArguments(final String scenario, final String casesPath, final DatabaseType databaseType, final String runMode) {
        return ENV.getClusterEnvironment().getAdapters().stream().flatMap(each -> getTestCaseArguments(scenario, casesPath, databaseType, runMode, each));
    }
    
    private Stream<? extends Arguments> getTestCaseArguments(final String scenario, final String casesPath, final DatabaseType databaseType, final String runMode, final String adapter) {
        ScriptReader reader = new ScriptReader(new InputStreamReader(Objects.requireNonNull(BusinessScenarioTestCaseArgumentsProvider.class.getClassLoader().getResourceAsStream(casesPath))));
        AtomicReference<String> useSql = new AtomicReference<>("");
        return Streams.of(new Iterator<String>() {
            
            private String nextStatement;
            
            @Override
            public boolean hasNext() {
                nextStatement = reader.readStatement();
                return null != nextStatement;
            }
            
            @Override
            public String next() {
                return nextStatement;
            }
        }).filter(StringUtils::isNotBlank).map(each -> getArguments(scenario, casesPath, databaseType, runMode, adapter, each, useSql)).filter(Objects::nonNull);
    }
    
    private Arguments getArguments(final String scenario, final String casesPath, final DatabaseType databaseType, final String runMode, final String adapter, final String sql,
                                   final AtomicReference<String> useSql) {
        ScriptReader scriptReader = new ScriptReader(new StringReader(sql));
        scriptReader.setSkipRemarks(true);
        String trimSql = scriptReader.readStatement().trim();
        if (trimSql.toLowerCase().startsWith("use")) {
            useSql.set(trimSql + ";");
            return null;
        }
        E2ETestCase testCase = new E2ETestCase();
        testCase.setSql(useSql.get() + sql);
        E2ETestCaseContext testCaseContext = new E2ETestCaseContext(testCase, casesPath);
        return Arguments.of(new CaseTestParameter(testCaseContext, adapter, scenario, runMode, databaseType, SQLCommandType.ALL));
    }
}
