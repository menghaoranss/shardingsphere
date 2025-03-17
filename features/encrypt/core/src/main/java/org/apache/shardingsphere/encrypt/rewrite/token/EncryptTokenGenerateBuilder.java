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

package org.apache.shardingsphere.encrypt.rewrite.token;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.rewrite.aware.SQLAware;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.insert.EncryptInsertSelectScalarSubqueryTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.insert.EncryptInsertSelectValueTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.insert.EncryptMultiTableInsertTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.merge.EncryptMergeTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.orderby.EncryptOrderByItemTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.predicate.EncryptCreateAlterViewPredicateColumnTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.predicate.EncryptCreateAlterViewPredicateValueTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.projection.EncryptCreateAlterViewSelectProjectionTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.select.EncryptCreateAlterViewGroupByItemTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.select.EncryptCreateAlterViewOrderByItemTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.select.EncryptSelectForUpdateTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.select.EncryptUsingNaturalJoinTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.with.EncryptWithColumnTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.table.generator.EncryptTableTokenGenerator;
import com.sphereex.dbplusengine.infra.rewrite.aware.ConfigurationPropertiesAware;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.rewrite.aware.EncryptConditionsAware;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptCondition;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.assignment.EncryptInsertAssignmentTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.assignment.EncryptUpdateAssignmentTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.ddl.EncryptAlterTableTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.ddl.EncryptCreateTableTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.insert.EncryptInsertCipherNameTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.insert.EncryptInsertDefaultColumnsTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.insert.EncryptInsertDerivedColumnsTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.insert.EncryptInsertOnUpdateTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.insert.EncryptInsertValuesTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.predicate.EncryptInsertPredicateColumnTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.predicate.EncryptInsertPredicateValueTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.predicate.EncryptPredicateColumnTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.predicate.EncryptPredicateValueTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.projection.EncryptInsertSelectProjectionTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.projection.EncryptSelectProjectionTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.select.EncryptGroupByItemTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.select.EncryptIndexColumnTokenGenerator;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.SQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.builder.SQLTokenGeneratorBuilder;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * SQL token generator builder for encrypt.
 */
@RequiredArgsConstructor
public final class EncryptTokenGenerateBuilder implements SQLTokenGeneratorBuilder {
    
    private final SQLStatementContext sqlStatementContext;
    
    private final Collection<EncryptCondition> encryptConditions;
    
    private final EncryptRule rule;
    
    @SphereEx
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    private final SQLRewriteContext sqlRewriteContext;
    
    @SphereEx
    private final ConfigurationProperties props;
    
    @Override
    public Collection<SQLTokenGenerator> getSQLTokenGenerators() {
        Collection<SQLTokenGenerator> result = new LinkedList<>();
        ShardingSphereDatabase database = sqlRewriteContext.getDatabase();
        @SphereEx
        ShardingSphereMetaData metaData = sqlRewriteContext.getMetaData();
        // SPEX CHANGED: BEGIN
        addSQLTokenGenerator(result, new EncryptSelectProjectionTokenGenerator(rule, databaseEncryptRules, database, metaData));
        addSQLTokenGenerator(result, new EncryptInsertSelectProjectionTokenGenerator(rule, databaseEncryptRules, database, metaData));
        addSQLTokenGenerator(result, new EncryptInsertAssignmentTokenGenerator(rule, databaseEncryptRules, database));
        addSQLTokenGenerator(result, new EncryptUpdateAssignmentTokenGenerator(rule, databaseEncryptRules, database));
        addSQLTokenGenerator(result, new EncryptPredicateColumnTokenGenerator(rule, databaseEncryptRules, database, metaData));
        addSQLTokenGenerator(result, new EncryptInsertPredicateColumnTokenGenerator(rule, databaseEncryptRules, database, metaData));
        addSQLTokenGenerator(result, new EncryptPredicateValueTokenGenerator(rule, databaseEncryptRules, database));
        addSQLTokenGenerator(result, new EncryptInsertPredicateValueTokenGenerator(rule, databaseEncryptRules, database));
        // SPEX CHANGED: END
        addSQLTokenGenerator(result, new EncryptInsertValuesTokenGenerator(rule, database));
        // SPEX CHANGED: BEGIN
        addSQLTokenGenerator(result, new EncryptInsertDefaultColumnsTokenGenerator(rule, databaseEncryptRules));
        addSQLTokenGenerator(result, new EncryptInsertCipherNameTokenGenerator(rule, databaseEncryptRules, database, metaData));
        // SPEX CHANGED: END
        addSQLTokenGenerator(result, new EncryptInsertDerivedColumnsTokenGenerator(rule));
        addSQLTokenGenerator(result, new EncryptInsertOnUpdateTokenGenerator(rule, database));
        // SPEX CHANGED: BEGIN
        addSQLTokenGenerator(result, new EncryptGroupByItemTokenGenerator(rule, database, metaData));
        addSQLTokenGenerator(result, new EncryptIndexColumnTokenGenerator(rule, database, metaData));
        // SPEX CHANGED: END
        addSQLTokenGenerator(result, new EncryptCreateTableTokenGenerator(rule));
        addSQLTokenGenerator(result, new EncryptAlterTableTokenGenerator(rule));
        // SPEX ADDED: BEGIN
        addSQLTokenGenerator(result, new EncryptOrderByItemTokenGenerator(rule, database, metaData));
        addSQLTokenGenerator(result, new EncryptUsingNaturalJoinTokenGenerator(rule, databaseEncryptRules));
        addSQLTokenGenerator(result, new EncryptSelectForUpdateTokenGenerator(rule, database, metaData));
        addSQLTokenGenerator(result, new EncryptMultiTableInsertTokenGenerator(rule, databaseEncryptRules, sqlRewriteContext));
        addSQLTokenGenerator(result, new EncryptMergeTokenGenerator(rule, databaseEncryptRules, database, metaData));
        addSQLTokenGenerator(result, new EncryptTableTokenGenerator(rule, database));
        addSQLTokenGenerator(result, new EncryptInsertSelectValueTokenGenerator(rule, database));
        addSQLTokenGenerator(result, new EncryptInsertSelectScalarSubqueryTokenGenerator(rule, databaseEncryptRules, sqlRewriteContext));
        addSQLTokenGenerator(result, new EncryptWithColumnTokenGenerator(rule, databaseEncryptRules, database, metaData));
        addSQLTokenGenerator(result, new EncryptCreateAlterViewSelectProjectionTokenGenerator(rule, databaseEncryptRules, database, metaData));
        addSQLTokenGenerator(result, new EncryptCreateAlterViewPredicateColumnTokenGenerator(rule, databaseEncryptRules, database, metaData));
        addSQLTokenGenerator(result, new EncryptCreateAlterViewPredicateValueTokenGenerator(rule, databaseEncryptRules, database));
        addSQLTokenGenerator(result, new EncryptCreateAlterViewGroupByItemTokenGenerator(rule, database, metaData));
        addSQLTokenGenerator(result, new EncryptCreateAlterViewOrderByItemTokenGenerator(rule, database, metaData));
        // SPEX ADDED: END
        return result;
    }
    
    private void addSQLTokenGenerator(final Collection<SQLTokenGenerator> sqlTokenGenerators, final SQLTokenGenerator toBeAddedSQLTokenGenerator) {
        setUpSQLTokenGenerator(toBeAddedSQLTokenGenerator);
        if (toBeAddedSQLTokenGenerator.isGenerateSQLToken(sqlStatementContext)) {
            sqlTokenGenerators.add(toBeAddedSQLTokenGenerator);
        }
    }
    
    private void setUpSQLTokenGenerator(final SQLTokenGenerator toBeAddedSQLTokenGenerator) {
        if (toBeAddedSQLTokenGenerator instanceof EncryptConditionsAware) {
            ((EncryptConditionsAware) toBeAddedSQLTokenGenerator).setEncryptConditions(encryptConditions);
        }
        // SPEX ADDED: BEGIN
        if (toBeAddedSQLTokenGenerator instanceof ConfigurationPropertiesAware) {
            ((ConfigurationPropertiesAware) toBeAddedSQLTokenGenerator).setConfigurationProperties(props);
        }
        if (toBeAddedSQLTokenGenerator instanceof SQLAware) {
            ((SQLAware) toBeAddedSQLTokenGenerator).setSQL(sqlRewriteContext.getSql());
        }
        // SPEX ADDED: END
    }
}
