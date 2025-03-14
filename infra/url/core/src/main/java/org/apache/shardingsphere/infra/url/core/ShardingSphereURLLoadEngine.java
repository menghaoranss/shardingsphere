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

package org.apache.shardingsphere.infra.url.core;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.infra.url.spi.ShardingSphereURLDirectoryLoader;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.infra.url.core.arg.URLArgumentLineRender;
import org.apache.shardingsphere.infra.url.core.arg.URLArgumentPlaceholderType;
import org.apache.shardingsphere.infra.url.core.arg.URLArgumentPlaceholderTypeFactory;
import org.apache.shardingsphere.infra.url.spi.ShardingSphereURLLoader;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ShardingSphere URL load engine.
 */
public final class ShardingSphereURLLoadEngine {
    
    private final ShardingSphereURL url;
    
    private final ShardingSphereURLLoader urlLoader;
    
    @SphereEx
    @SuppressWarnings("rawtypes")
    private final ShardingSphereURLDirectoryLoader urlDirectoryLoader;
    
    public ShardingSphereURLLoadEngine(final ShardingSphereURL url) {
        this.url = url;
        urlLoader = TypedSPILoader.getService(ShardingSphereURLLoader.class, url.getSourceType());
        urlDirectoryLoader = TypedSPILoader.getService(ShardingSphereURLDirectoryLoader.class, url.getSourceType());
    }
    
    /**
     * Load configuration content.
     *
     * @return loaded content
     */
    public byte[] loadContent() {
        Collection<String> lines = Arrays.asList(urlLoader.load(url.getConfigurationSubject(), url.getQueryProps()).split(System.lineSeparator()));
        return URLArgumentLineRender.render(lines, URLArgumentPlaceholderTypeFactory.valueOf(url.getQueryProps()));
    }
    
    /**
     * Is directory or not.
     *
     * @return directory or not
     */
    @SphereEx
    public boolean isDirectory() {
        return urlDirectoryLoader.isDirectory(url.getConfigurationSubject(), url.getQueryProps());
    }
    
    /**
     * Load configuration contents.
     *
     * @return loaded contents
     */
    @SuppressWarnings("unchecked")
    @SphereEx
    public Map<String, byte[]> loadContents() {
        URLArgumentPlaceholderType placeholderType = URLArgumentPlaceholderTypeFactory.valueOf(url.getQueryProps());
        Collection<?> allConfigurationSubjects = urlDirectoryLoader.getAllConfigurationSubjects(url.getConfigurationSubject(), url.getQueryProps());
        return allConfigurationSubjects.stream().collect(Collectors.toMap(Object::toString,
                each -> URLArgumentLineRender.render(
                        Arrays.asList(urlDirectoryLoader.loadIndicate(url.getConfigurationSubject(), url.getQueryProps(), urlLoader, each).split(System.lineSeparator())), placeholderType)));
    }
}
