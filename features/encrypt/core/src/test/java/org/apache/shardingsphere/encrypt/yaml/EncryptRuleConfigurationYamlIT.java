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

package org.apache.shardingsphere.encrypt.yaml;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeType;
import com.sphereex.dbplusengine.encrypt.config.rule.PlainColumnItemRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.yaml.config.rule.YamlEncryptModeRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.EncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnItemRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.encrypt.yaml.config.YamlEncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.yaml.config.rule.YamlEncryptTableRuleConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.yaml.YamlAlgorithmConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.YamlRuleConfiguration;
import org.apache.shardingsphere.test.it.yaml.YamlRuleConfigurationIT;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class EncryptRuleConfigurationYamlIT extends YamlRuleConfigurationIT {
    
    EncryptRuleConfigurationYamlIT() {
        super("yaml/encrypt-rule.yaml", getExpectedRuleConfiguration());
    }
    
    private static EncryptRuleConfiguration getExpectedRuleConfiguration() {
        EncryptColumnRuleConfiguration encryptColumnRuleConfig = new EncryptColumnRuleConfiguration("username",
                new EncryptColumnItemRuleConfiguration("username_cipher", "aes_encryptor"));
        encryptColumnRuleConfig.setAssistedQuery(new EncryptColumnItemRuleConfiguration("assisted_query_username", "assisted_encryptor"));
        // SPEX ADDED: BEGIN
        encryptColumnRuleConfig.setPlain(new PlainColumnItemRuleConfiguration("username"));
        encryptColumnRuleConfig.setDataType("VARCHAR (50)");
        // SPEX ADDED: END
        EncryptTableRuleConfiguration tableRuleConfig = new EncryptTableRuleConfiguration("t_user", Collections.singletonList(encryptColumnRuleConfig));
        // SPEX ADDED: BEGIN
        tableRuleConfig.setRenameTable("spex_t_user");
        // SPEX ADDED: END
        Collection<EncryptTableRuleConfiguration> tables = Collections.singletonList(tableRuleConfig);
        Map<String, AlgorithmConfiguration> encryptors = new LinkedHashMap<>(2, 1F);
        encryptors.put("aes_encryptor", new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "123456abc"), new Property("digest-algorithm-name", "SHA-1"))));
        encryptors.put("assisted_encryptor", new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "123456abc"), new Property("digest-algorithm-name", "SHA-1"))));
        // SPEX CHANGED: BEGIN
        EncryptRuleConfiguration result = new EncryptRuleConfiguration(tables, encryptors);
        result.setEncryptMode(new EncryptModeRuleConfiguration(EncryptModeType.FRONTEND, PropertiesBuilder.build(new Property("udf-sql-enabled", true), new Property("udf-routine-enabled", false),
                new Property("udf-view-enabled", false), new Property("rename-table-prefix", "SPEX_"))));
        return result;
        // SPEX CHANGED: END
    }
    
    @Override
    protected boolean assertYamlConfiguration(final YamlRuleConfiguration actual) {
        assertEncryptRule((YamlEncryptRuleConfiguration) actual);
        return true;
    }
    
    private void assertEncryptRule(final YamlEncryptRuleConfiguration actual) {
        assertTables(actual.getTables());
        assertEncryptAlgorithm(actual.getEncryptors());
        // SPEX ADDED: BEGIN
        assertEncryptMode(actual.getEncryptMode());
        // SPEX ADDED: END
    }
    
    private void assertTables(final Map<String, YamlEncryptTableRuleConfiguration> actual) {
        assertThat(actual.size(), is(1));
        assertThat(actual.get("t_user").getColumns().size(), is(1));
        assertThat(actual.get("t_user").getColumns().get("username").getCipher().getName(), is("username_cipher"));
        assertThat(actual.get("t_user").getColumns().get("username").getCipher().getEncryptorName(), is("aes_encryptor"));
        assertThat(actual.get("t_user").getColumns().get("username").getAssistedQuery().getName(), is("assisted_query_username"));
        assertThat(actual.get("t_user").getColumns().get("username").getAssistedQuery().getEncryptorName(), is("assisted_encryptor"));
        // SPEX ADDED: BEGIN
        assertThat(actual.get("t_user").getColumns().get("username").getPlain().getName(), is("username"));
        assertThat(actual.get("t_user").getColumns().get("username").getDataType(), is("VARCHAR (50)"));
        assertThat(actual.get("t_user").getRenameTable(), is("spex_t_user"));
        // SPEX ADDED: END
    }
    
    private void assertEncryptAlgorithm(final Map<String, YamlAlgorithmConfiguration> actual) {
        assertThat(actual.size(), is(2));
        assertThat(actual.get("aes_encryptor").getType(), is("AES"));
        assertThat(actual.get("aes_encryptor").getProps().getProperty("aes-key-value"), is("123456abc"));
        assertThat(actual.get("aes_encryptor").getProps().getProperty("digest-algorithm-name"), is("SHA-1"));
        assertThat(actual.get("assisted_encryptor").getType(), is("AES"));
        assertThat(actual.get("assisted_encryptor").getProps().getProperty("aes-key-value"), is("123456abc"));
        assertThat(actual.get("assisted_encryptor").getProps().getProperty("digest-algorithm-name"), is("SHA-1"));
    }
    
    @SphereEx
    private void assertEncryptMode(final YamlEncryptModeRuleConfiguration encryptMode) {
        assertThat(encryptMode.getType(), is("FRONTEND"));
        assertThat(Boolean.parseBoolean(encryptMode.getProps().getOrDefault("udf-sql-enabled", "false").toString()), is(true));
        assertThat(Boolean.parseBoolean(encryptMode.getProps().getOrDefault("udf-routine-enabled", "false").toString()), is(false));
        assertThat(Boolean.parseBoolean(encryptMode.getProps().getOrDefault("udf-view-enabled", "false").toString()), is(false));
        assertThat(encryptMode.getProps().getProperty("rename-table-prefix"), is("SPEX_"));
    }
}
