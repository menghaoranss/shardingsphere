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

package com.sphereex.dbplusengine.infra.url.spi;

import org.apache.shardingsphere.infra.spi.annotation.SingletonSPI;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPI;
import org.apache.shardingsphere.infra.url.spi.ShardingSphereURLLoader;

import java.util.Collection;
import java.util.Properties;

/**
 * ShardingSphere URL directory loader.
 * 
 * @param <T> type of indicated subject
 */
@SingletonSPI
public interface ShardingSphereURLDirectoryLoader<T> extends TypedSPI {
    
    /**
     * Is directory or not.
     *
     * @param configurationSubject configuration subject
     * @param queryProps query properties
     * @return directory or not
     */
    boolean isDirectory(String configurationSubject, Properties queryProps);
    
    /**
     * Get all configuration subjects.
     *
     * @param configurationSubject configuration subject
     * @param queryProps query properties
     * @return all configuration subjects
     */
    Collection<T> getAllConfigurationSubjects(String configurationSubject, Properties queryProps);
    
    /**
     * Load indicate content.
     *
     * @param configurationSubject configuration subject
     * @param queryProps query properties
     * @param urlLoader URL loader
     * @param indicatedSubject indicated subject
     * @return loaded content
     */
    String loadIndicate(String configurationSubject, Properties queryProps, ShardingSphereURLLoader urlLoader, T indicatedSubject);
}
