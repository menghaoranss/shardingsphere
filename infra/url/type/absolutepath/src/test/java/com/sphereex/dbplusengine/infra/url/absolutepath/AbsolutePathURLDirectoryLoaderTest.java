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

package com.sphereex.dbplusengine.infra.url.absolutepath;

import org.apache.shardingsphere.infra.url.absolutepath.AbsolutePathURLLoader;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Objects;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbsolutePathURLDirectoryLoaderTest {
    
    private final AbsolutePathURLDirectoryLoader loader = new AbsolutePathURLDirectoryLoader();
    
    @Test
    void assertIsDirectory() {
        assertTrue(loader.isDirectory(getConfigurationPath(), new Properties()));
    }
    
    @Test
    void assertGetAllConfigurationSubjects() {
        Collection<String> actual = loader.getAllConfigurationSubjects(getConfigurationPath(), new Properties());
        assertThat(actual.size(), is(1));
        assertThat(actual.iterator().next(), is("fixture.yaml"));
    }
    
    @Test
    void assertLoadIndicate() {
        String actual = loader.loadIndicate(getConfigurationPath(), new Properties(), new AbsolutePathURLLoader(), "fixture.yaml");
        assertFalse(actual.isEmpty());
    }
    
    private String getConfigurationPath() {
        return Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("config/absolutepath/")).getPath();
    }
}
