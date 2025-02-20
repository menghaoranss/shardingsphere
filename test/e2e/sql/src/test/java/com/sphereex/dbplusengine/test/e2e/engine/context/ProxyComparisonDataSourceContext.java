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

package com.sphereex.dbplusengine.test.e2e.engine.context;

import com.google.common.base.Joiner;
import com.sphereex.dbplusengine.test.e2e.engine.hook.SqlExecuteHook;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.test.e2e.env.container.atomic.adapter.AdapterContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.adapter.config.AdaptorContainerConfiguration;
import org.apache.shardingsphere.test.e2e.env.container.atomic.adapter.impl.ShardingSphereProxyStandaloneContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.constants.ProxyContainerConstants;
import org.apache.shardingsphere.test.e2e.env.container.atomic.util.AdapterContainerUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
public final class ProxyComparisonDataSourceContext extends ComparisonDataSourceContext {
    
    private static final String REGISTER_STORAGE_UNIT_DIST_SQL = "REGISTER STORAGE UNIT %s ( HOST=\"%s\", PORT=%s, DB=\"%s\", USER=\"%s\", PASSWORD=\"%s\")\n";
    
    private static final String CREATE_DATABASE_SQL = "CREATE DATABASE %s";
    
    private static final String CREATE_ENCRYPT_KEY_MANAGER_DIST_SQL = "CREATE ENCRYPT KEY MANAGER local_key_manage (TYPE(NAME='LOCAL',PROPERTIES(\"transform-indexes\"=\"1,2,3,5,7,14\")))";
    
    private static final String CREATE_ENCRYPT_RULE_COLUMNS_DIST_SQL = "(NAME=`%s`, DATA_TYPE='%s', CIPHER=`%s`, CIPHER_DATA_TYPE='%s', LIKE_QUERY=`%s`, LIKE_QUERY_DATA_TYPE='%s', ORDER_QUERY=`%s`, "
            + "ORDER_QUERY_DATA_TYPE='%s', ENCRYPT_ALGORITHM(TYPE(NAME='AES',PROPERTIES('aes-key-value'='123456', 'digest-algorithm-name'='SHA-256'))), "
            + "LIKE_QUERY_ALGORITHM(TYPE(NAME='SphereEx:CHAR_TRANSFORM_LIKE',PROPERTIES('key-manager'='local_key_manage'))), "
            + "ORDER_QUERY_ALGORITHM(TYPE(NAME='SphereEx:FASTOPE',PROPERTIES('alpha-key'='0.8935217796678353','factor-e-key'='0.9186430364852792','factor-k-key'='523211953918290'))), "
            + "QUERY_WITH_CIPHER_COLUMN=true)";
    
    private static final String DROP_ENCRYPT_RULE_DIST_SQL = "DROP ENCRYPT RULE IF EXISTS ";
    
    public ProxyComparisonDataSourceContext(final String defaultDatabaseName, final DatabaseType databaseType, final Collection<SqlExecuteHook> sqlExecuteHooks) {
        super(defaultDatabaseName, databaseType, sqlExecuteHooks);
    }
    
    @Override
    protected Optional<AdapterContainer> generateAdapterContainer() {
        return Optional.of(getProxyStandaloneContainer(getDatabaseType()));
    }
    
    @SneakyThrows(SQLException.class)
    @Override
    protected DataSource generateTargetDataSource() {
        DataSource result = getAdapterContainer().getTargetDataSource("");
        try (Connection connection = result.getConnection()) {
            createProxyDatabase(getCurrentDatabaseName(), connection);
            initProxyDatabase(getCurrentDatabaseName(), connection);
        }
        return result;
    }
    
    private ShardingSphereProxyStandaloneContainer getProxyStandaloneContainer(final DatabaseType databaseType) {
        Map<String, String> mountedResources = new HashMap<>();
        mountedResources.put("/env/common/standalone/proxy/conf/global.yaml", ProxyContainerConstants.CONFIG_PATH_IN_CONTAINER + "global.yaml");
        mountedResources.put("/env/common/sphere-ex.license", ProxyContainerConstants.CONFIG_PATH_IN_CONTAINER + "sphere-ex.license");
        mountedResources.put("/env/common/standalone/proxy/conf/logback.xml", ProxyContainerConstants.CONFIG_PATH_IN_CONTAINER + "logback.xml");
        return new ShardingSphereProxyStandaloneContainer(databaseType,
                new AdaptorContainerConfiguration("", new ArrayList<>(), mountedResources, AdapterContainerUtils.getAdapterContainerImage(), ""));
    }
    
    private void createProxyDatabase(final String databaseName, final Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format(CREATE_DATABASE_SQL, databaseName));
        }
    }
    
    /**
     * Init proxy database.
     *
     * @param databaseName database name
     */
    @SneakyThrows(SQLException.class)
    public void initProxyDatabase(final String databaseName) {
        try (Connection connection = getTargetConnection()) {
            initProxyDatabase(databaseName, connection);
        }
    }
    
    private void initProxyDatabase(final String databaseName, final Connection connection) throws SQLException {
        createSchemaIfNotExist(getActualDataSource(), databaseName);
        connection.setCatalog(databaseName);
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_ENCRYPT_KEY_MANAGER_DIST_SQL);
            registerStorageUnit(statement, databaseName);
        }
    }
    
    private void registerStorageUnit(final Statement statement, final String databaseName) throws SQLException {
        String host = getContainerComposer().getActualStorageContainer().getNetworkAliases().get(0);
        int port = getContainerComposer().getActualStorageContainer().getExposedPort();
        String username = getContainerComposer().getActualStorageContainer().getUsername();
        String password = getContainerComposer().getActualStorageContainer().getPassword();
        statement.execute(String.format(REGISTER_STORAGE_UNIT_DIST_SQL, databaseName, host, port, databaseName, username, password));
    }
    
    @SneakyThrows(SQLException.class)
    @Override
    public void addEncryptTableRule(final EncryptTableRuleConfiguration encryptTableRuleConfig) {
        String table = encryptTableRuleConfig.getName();
        StringBuilder distSqlBuilder = new StringBuilder("CREATE ENCRYPT RULE `" + table + "` (COLUMNS(");
        for (EncryptColumnRuleConfiguration column : encryptTableRuleConfig.getColumns()) {
            distSqlBuilder.append(String.format(CREATE_ENCRYPT_RULE_COLUMNS_DIST_SQL, column.getName(), column.getDataType().get(), column.getCipher().getName(),
                    column.getCipher().getDataType().get(), column.getLikeQuery().get().getName(), column.getLikeQuery().get().getDataType().get(), column.getOrderQuery().get().getName(),
                    column.getOrderQuery().get().getDataType().get()));
            distSqlBuilder.append(",");
        }
        distSqlBuilder.deleteCharAt(distSqlBuilder.length() - 1);
        distSqlBuilder.append("))");
        try (Connection connection = getTargetConnection(); Statement statement = connection.createStatement()) {
            String distSql = distSqlBuilder.toString();
            log.info(distSql);
            statement.execute(distSql);
        }
    }
    
    @SneakyThrows(SQLException.class)
    @Override
    public void removeEncryptTableRule(final Collection<String> ruleNames) {
        String distSql = DROP_ENCRYPT_RULE_DIST_SQL + Joiner.on(",").join(ruleNames);
        try (Connection connection = getTargetConnection(); Statement statement = connection.createStatement()) {
            log.info(distSql);
            statement.execute(distSql);
        }
    }
}
