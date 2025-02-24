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

package com.sphereex.dbplusengine.parser.warmup.engine;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sphereex.dbplusengine.parser.warmup.api.yaml.YamlSQLWarmupConfiguration;
import com.sphereex.dbplusengine.parser.warmup.config.SQLWarmupConfiguration;
import com.sphereex.dbplusengine.parser.warmup.config.SQLWarmupConfiguration.ContentPlaceholderConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.parser.SQLParserEngine;
import org.apache.shardingsphere.infra.url.core.ShardingSphereURL;
import org.apache.shardingsphere.infra.url.core.ShardingSphereURLLoadEngine;
import org.apache.shardingsphere.infra.util.yaml.YamlEngine;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL warmup engine.
 */
@RequiredArgsConstructor
@Slf4j
public final class SQLWarmupEngine {
    
    private final SQLParserEngine sqlParserEngine;
    
    private final ConfigurationProperties props;
    
    /**
     * Warmup init SQL.
     */
    public void warmupInitSQL() {
        String warmupSqlUrl = props.getValue(ConfigurationPropertyKey.WARMUP_SQL_URL);
        if (!Strings.isNullOrEmpty(warmupSqlUrl)) {
            doWarmup(getWarmupSQL(warmupSqlUrl));
        }
    }
    
    @SneakyThrows(IOException.class)
    private Collection<String> getWarmupSQL(final String warmupSqlUrl) {
        ShardingSphereURL url = ShardingSphereURL.parse(warmupSqlUrl);
        return generateSQL(YamlEngine.unmarshal(new ShardingSphereURLLoadEngine(url).loadContent(), YamlSQLWarmupConfiguration.class));
    }
    
    private Collection<String> generateSQL(final YamlSQLWarmupConfiguration warmupConfig) {
        Collection<String> result = new LinkedList<>();
        for (SQLWarmupConfiguration each : warmupConfig.getSqlWarmup()) {
            result.addAll(generateSQL(each));
        }
        return result;
    }
    
    private Collection<String> generateSQL(final SQLWarmupConfiguration warmupConfig) {
        int formatCount = getFormatCount(warmupConfig.getSql());
        if (0 == formatCount) {
            return Collections.singleton(warmupConfig.getSql());
        }
        fillEmptyWarmupConfigContents(formatCount, warmupConfig);
        Preconditions.checkState(formatCount == warmupConfig.getContents().size(), "The (%s) count and contents count not match.");
        Collection<String> result = new LinkedList<>();
        for (int i = 1; i <= warmupConfig.getLimit(); i++) {
            result.add(generateSQL(warmupConfig.getSql(), formatCount, i, warmupConfig.getContents()));
        }
        return result;
    }
    
    private String generateSQL(final String sql, final int formatCount, final int limit, final Collection<ContentPlaceholderConfiguration> contents) {
        List<ContentPlaceholderConfiguration> contentPlaceholderConfigs = new LinkedList<>(contents);
        String[] placeholders = new String[formatCount];
        for (int i = 0; i < formatCount; i++) {
            placeholders[i] = generatePlaceholderSQL(limit, contentPlaceholderConfigs.get(i));
        }
        return replacePlaceholders(sql, placeholders);
    }
    
    private int getFormatCount(final String sql) {
        Matcher matcher = Pattern.compile(Pattern.quote("(%s)")).matcher(sql);
        int result = 0;
        while (matcher.find()) {
            result++;
        }
        return result;
    }
    
    private void fillEmptyWarmupConfigContents(final int formatCount, final SQLWarmupConfiguration warmupConfig) {
        if (!warmupConfig.getContents().isEmpty()) {
            return;
        }
        for (int i = 0; i < formatCount; i++) {
            warmupConfig.getContents().add(new ContentPlaceholderConfiguration());
        }
    }
    
    private String replacePlaceholders(final String sql, final String... replacements) {
        if (0 == replacements.length) {
            return sql;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < replacements.length; i++) {
            sb.append(Pattern.quote("%s"));
            if (i < replacements.length - 1) {
                sb.append("|");
            }
        }
        Matcher matcher = Pattern.compile(sb.toString()).matcher(sql);
        StringBuffer result = new StringBuffer();
        int replacementIndex = 0;
        while (matcher.find()) {
            String replacement = Matcher.quoteReplacement(replacements[replacementIndex]);
            matcher.appendReplacement(result, replacement);
            replacementIndex++;
        }
        matcher.appendTail(result);
        return result.toString();
    }
    
    private String generatePlaceholderSQL(final int limit, final ContentPlaceholderConfiguration placeholderConfig) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < limit; i++) {
            result.append(placeholderConfig.getBefore());
            result.append("?");
            if (i < limit - 1) {
                result.append(placeholderConfig.getAfter());
                result.append(",");
            }
        }
        return result.toString();
    }
    
    private void doWarmup(final Collection<String> warmupSQLs) {
        Collection<ForkJoinTask<?>> futures = new LinkedList<>();
        for (String each : warmupSQLs) {
            futures.add(ForkJoinPool.commonPool().submit(() -> {
                log.info("Warmup SQL is {}", each);
                sqlParserEngine.parse(each, true);
            }));
        }
        futures.forEach(ForkJoinTask::join);
    }
}
