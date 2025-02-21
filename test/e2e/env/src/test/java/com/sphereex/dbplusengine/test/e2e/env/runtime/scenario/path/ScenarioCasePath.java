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

package com.sphereex.dbplusengine.test.e2e.env.runtime.scenario.path;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.util.directory.ClasspathResourceDirectoryReader;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Scenario common path.
 */
@RequiredArgsConstructor
public final class ScenarioCasePath {
    
    private static final String ROOT_PATH = "env/scenario";
    
    private static final String CASES_PATH = "cases";
    
    private final String scenario;
    
    /**
     * Get rule configuration file.
     *
     * @return rule configuration file
     */
    public Collection<String> getCasesPaths() {
        return getFile(String.join("/", ROOT_PATH, scenario, CASES_PATH));
    }
    
    private Collection<String> getFile(final String path) {
        return ClasspathResourceDirectoryReader.read(path).collect(Collectors.toList());
    }
}
