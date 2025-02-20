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

import com.sphereex.dbplusengine.infra.util.DatabaseTypeUtils;
import com.sphereex.dbplusengine.test.e2e.engine.composer.ExternalE2EContainerComposer;
import com.sphereex.dbplusengine.test.e2e.engine.hook.SqlExecuteHook;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.shardingsphere.encrypt.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.test.e2e.env.container.atomic.ITContainers;
import org.apache.shardingsphere.test.e2e.env.container.atomic.adapter.AdapterContainer;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Optional;

@Getter
public abstract class ComparisonDataSourceContext {
    
    @Setter
    private String currentDatabaseName;
    
    private final String defaultDatabaseName;
    
    private final DatabaseType databaseType;
    
    private ITContainers itContainers = new ITContainers(null);
    
    private ExternalE2EContainerComposer containerComposer;
    
    @Setter
    private DataSource targetDataSource;
    
    private DataSource actualDataSource;
    
    private DataSource expectDataSource;
    
    private final Collection<SqlExecuteHook> sqlExecuteHooks;
    
    private AdapterContainer adapterContainer;
    
    public ComparisonDataSourceContext(final String defaultDatabaseName, final DatabaseType databaseType, final Collection<SqlExecuteHook> sqlExecuteHooks) {
        this.defaultDatabaseName = defaultDatabaseName;
        this.currentDatabaseName = defaultDatabaseName;
        this.databaseType = databaseType;
        this.adapterContainer = generateAdapterContainer().map(itContainers::registerContainer).orElse(null);
        this.containerComposer = new ExternalE2EContainerComposer(databaseType, this.itContainers);
        this.actualDataSource = generateActualDataSource();
        this.targetDataSource = generateTargetDataSource();
        this.expectDataSource = generateExpectDataSource();
        this.sqlExecuteHooks = sqlExecuteHooks;
    }
    
    /**
     * reset.
     */
    @SneakyThrows(IOException.class)
    public void reset() {
        this.currentDatabaseName = defaultDatabaseName;
        itContainers.close();
        itContainers = new ITContainers(null);
        this.adapterContainer = generateAdapterContainer().map(itContainers::registerContainer).orElse(null);
        this.containerComposer = new ExternalE2EContainerComposer(databaseType, this.itContainers);
        closeDataSource(this.actualDataSource);
        this.actualDataSource = generateActualDataSource();
        closeDataSource(this.targetDataSource);
        this.targetDataSource = generateTargetDataSource();
        closeDataSource(this.expectDataSource);
        this.expectDataSource = generateExpectDataSource();
    }
    
    private void closeDataSource(final DataSource dataSource) throws IOException {
        if (dataSource instanceof Closeable) {
            ((Closeable) dataSource).close();
        }
    }
    
    protected Optional<AdapterContainer> generateAdapterContainer() {
        return Optional.empty();
    }
    
    abstract DataSource generateTargetDataSource();
    
    @SneakyThrows
    private DataSource generateActualDataSource() {
        return containerComposer.createActualDataSource("");
    }
    
    @SneakyThrows
    private DataSource generateExpectDataSource() {
        DataSource result = containerComposer.createExpectedDataSource("");
        createSchemaIfNotExist(result, currentDatabaseName);
        return result;
    }
    
    /**
     * Get target connection.
     *
     * @return connection
     */
    @SuppressWarnings("checkstyle:IllegalCatch")
    @SneakyThrows(SQLException.class)
    public Connection getTargetConnection() {
        Connection result = targetDataSource.getConnection();
        if (StringUtils.isNotBlank(getCurrentDatabaseName())) {
            try {
                result.setCatalog(getCurrentDatabaseName());
                // CHECKSTYLE:OFF
            } catch (Throwable ex) {
                // CHECKSTYLE:ON
                result.close();
                throw ex;
            }
        }
        return result;
    }
    
    /**
     * Get target connection.
     *
     * @return connection
     */
    @SneakyThrows(SQLException.class)
    public Connection getExpectConnection() {
        Connection result = expectDataSource.getConnection();
        if (StringUtils.isNotBlank(getCurrentDatabaseName())) {
            try {
                result.setCatalog(getCurrentDatabaseName());
                // CHECKSTYLE:OFF
            } catch (Throwable ex) {
                // CHECKSTYLE:ON
                result.close();
                throw ex;
            }
        }
        return result;
    }
    
    @SneakyThrows(SQLException.class)
    protected void createSchemaIfNotExist(final DataSource dataSource, final String schema) {
        try (Connection connection = dataSource.getConnection()) {
            if (!isSchemaExist(schema, connection)) {
                try (Statement statement = connection.createStatement()) {
                    if (DatabaseTypeUtils.isOracleDatabase(databaseType)) {
                        statement.execute("CREATE USER " + schema + " identified by " + schema);
                        statement.execute("ALTER USER " + schema + " quota unlimited on users");
                        statement.execute("ALTER USER " + schema + " quota unlimited on SYSTEM");
                    } else {
                        statement.execute("CREATE SCHEMA " + schema);
                    }
                }
            }
        }
    }
    
    private static boolean isSchemaExist(final String schema, final Connection connection) throws SQLException {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            while (resultSet.next()) {
                if (schema.equalsIgnoreCase(resultSet.getString(1))) {
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * Add encrypt table rule config.
     *
     * @param encryptTableRuleConfig encryptTableRuleConfig
     */
    public abstract void addEncryptTableRule(EncryptTableRuleConfiguration encryptTableRuleConfig);
    
    /**
     * Remove encrypt table rule config.
     *
     * @param ruleNames rule names
     */
    public abstract void removeEncryptTableRule(Collection<String> ruleNames);
    
    /**
     * Do test.
     *
     * @param caseId case id
     * @param sql sql
     * @return whether execute test
     */
    public boolean doTest(final String caseId, final String sql) {
        for (SqlExecuteHook each : sqlExecuteHooks) {
            if (!each.test(caseId, sql, this)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Do before hook.
     *
     * @param sql sql
     * @return the SQL that may have been modified
     */
    public String doBeforeHook(final String sql) {
        String result = sql;
        for (SqlExecuteHook each : sqlExecuteHooks) {
            result = each.before(result, this);
        }
        return result;
    }
    
    /**
     * Do after hook.
     *
     * @param sql sql
     */
    public void doAfterHook(final String sql) {
        for (SqlExecuteHook each : sqlExecuteHooks) {
            each.after(sql, this);
        }
    }
}
