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

package org.apache.shardingsphere.mode.metadata.persist.config.global;

import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.YamlRuleConfiguration;
import org.apache.shardingsphere.infra.yaml.config.swapper.rule.YamlRuleConfigurationSwapperEngine;
import org.apache.shardingsphere.mode.metadata.persist.version.MetaDataVersionPersistService;
import org.apache.shardingsphere.mode.node.path.type.global.GlobalRuleNodePath;
import org.apache.shardingsphere.mode.node.path.type.version.VersionNodePath;
import org.apache.shardingsphere.mode.node.rule.tuple.RuleRepositoryTuple;
import org.apache.shardingsphere.mode.node.rule.tuple.YamlRuleRepositoryTupleSwapperEngine;
import org.apache.shardingsphere.mode.spi.repository.PersistRepository;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Global rule persist service.
 */
public final class GlobalRulePersistService {
    
    private final MetaDataVersionPersistService metaDataVersionPersistService;
    
    private final GlobalRuleRepositoryTuplePersistService ruleRepositoryTuplePersistService;
    
    private final YamlRuleRepositoryTupleSwapperEngine yamlRuleRepositoryTupleSwapperEngine;
    
    private final YamlRuleConfigurationSwapperEngine yamlRuleConfigurationSwapperEngine;
    
    public GlobalRulePersistService(final PersistRepository repository, final MetaDataVersionPersistService metaDataVersionPersistService) {
        this.metaDataVersionPersistService = metaDataVersionPersistService;
        ruleRepositoryTuplePersistService = new GlobalRuleRepositoryTuplePersistService(repository);
        yamlRuleRepositoryTupleSwapperEngine = new YamlRuleRepositoryTupleSwapperEngine();
        yamlRuleConfigurationSwapperEngine = new YamlRuleConfigurationSwapperEngine();
    }
    
    /**
     * Load global rule configurations.
     *
     * @return global rule configurations
     */
    public Collection<RuleConfiguration> load() {
        return yamlRuleRepositoryTupleSwapperEngine.swapToRuleConfigurations(ruleRepositoryTuplePersistService.load());
    }
    
    /**
     * Load global rule configuration.
     *
     * @param ruleType rule type to be loaded
     * @return global rule configuration
     */
    public Optional<RuleConfiguration> load(final String ruleType) {
        return yamlRuleRepositoryTupleSwapperEngine.swapToRuleConfiguration(ruleType, Collections.singleton(ruleRepositoryTuplePersistService.load(ruleType)));
    }
    
    /**
     * Persist global rule configurations.
     *
     * @param globalRuleConfigs global rule configurations
     */
    public void persist(final Collection<RuleConfiguration> globalRuleConfigs) {
        for (YamlRuleConfiguration each : yamlRuleConfigurationSwapperEngine.swapToYamlRuleConfigurations(globalRuleConfigs)) {
            persistTuples(yamlRuleRepositoryTupleSwapperEngine.swapToTuples(each));
        }
    }
    
    private void persistTuples(final Collection<RuleRepositoryTuple> tuples) {
        tuples.forEach(each -> metaDataVersionPersistService.persist(new VersionNodePath(new GlobalRuleNodePath(each.getKey())), each.getValue()));
    }
}
