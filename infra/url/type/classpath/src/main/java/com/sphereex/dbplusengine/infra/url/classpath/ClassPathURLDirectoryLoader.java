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

package com.sphereex.dbplusengine.infra.url.classpath;

import com.sphereex.dbplusengine.infra.url.spi.ShardingSphereURLDirectoryLoader;
import org.apache.commons.lang3.StringUtils;
import org.apache.shardingsphere.infra.url.spi.ShardingSphereURLLoader;
import org.apache.shardingsphere.infra.util.directory.ClasspathResourceDirectoryReader;

import java.io.File;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class path URL directory loader.
 */
public final class ClassPathURLDirectoryLoader implements ShardingSphereURLDirectoryLoader<String> {
    
    @Override
    public boolean isDirectory(final String configurationSubject, final Properties queryProps) {
        return ClasspathResourceDirectoryReader.isDirectory(configurationSubject);
    }
    
    @Override
    public Collection<String> getAllConfigurationSubjects(final String configurationSubject, final Properties queryProps) {
        try (Stream<String> resourceNameStream = ClasspathResourceDirectoryReader.read(configurationSubject)) {
            return resourceNameStream.map(resourceName -> StringUtils.removeStart(resourceName, configurationSubject + "/")).collect(Collectors.toList());
        }
    }
    
    @Override
    public String loadIndicate(final String configurationSubject, final Properties queryProps, final ShardingSphereURLLoader urlLoader, final String indicatedSubject) {
        return urlLoader.load(configurationSubject + File.separator + indicatedSubject, queryProps);
    }
    
    @Override
    public String getType() {
        return "classpath:";
    }
}
