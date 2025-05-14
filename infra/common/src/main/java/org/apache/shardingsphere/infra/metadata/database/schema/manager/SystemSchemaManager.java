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

package org.apache.shardingsphere.infra.metadata.database.schema.manager;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.cedarsoftware.util.CaseInsensitiveSet;
import com.sphereex.dbplusengine.SphereEx;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.shardingsphere.infra.util.directory.ClasspathResourceDirectoryReader;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * System schema manager.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SystemSchemaManager {
    
    private static final Map<String, Map<String, Collection<String>>> DATABASE_TYPE_SCHEMA_TABLE_MAP;
    
    private static final Map<String, Map<String, Collection<String>>> DATABASE_TYPE_SCHEMA_RESOURCE_MAP;
    
    private static final String COMMON = "common";
    
    @SphereEx
    private static final Collection<String> ORACLE_PUBLIC = new HashSet<>(Arrays.asList("all$olap2_aws", "all_all_tables", "all_apply", "all_apply_change_handlers", "all_apply_conflict_columns",
            "all_apply_dml_handlers", "all_apply_enqueue", "all_apply_error", "all_apply_execute", "all_apply_key_columns", "all_apply_parameters", "all_apply_progress", "all_apply_table_columns",
            "all_arguments", "all_assemblies", "all_associations", "all_attribute_transformations", "all_audit_policies", "all_audit_policy_columns", "all_aws", "all_aw_ac", "all_aw_ac_10g",
            "all_aw_obj", "all_aw_prop", "all_aw_prop_name", "all_aw_ps", "all_base_table_mviews", "all_capture", "all_capture_extra_attributes", "all_capture_parameters",
            "all_capture_prepared_database", "all_capture_prepared_schemas", "all_capture_prepared_tables", "all_catalog", "all_change_propagations", "all_change_propagation_sets", "all_change_sets",
            "all_change_sources", "all_change_tables", "all_clusters", "all_cluster_hash_expressions", "all_coll_types", "all_col_comments", "all_col_pending_stats", "all_col_privs",
            "all_col_privs_made", "all_col_privs_recd", "all_constraints", "all_cons_columns", "all_cons_obj_columns", "all_context", "all_cubes", "all_cube_attributes", "all_cube_attr_visibility",
            "all_cube_build_processes", "all_cube_calculated_members", "all_cube_dimensionality", "all_cube_dimensions", "all_cube_dim_levels", "all_cube_dim_models", "all_cube_dim_views",
            "all_cube_dim_view_columns", "all_cube_hierarchies", "all_cube_hier_levels", "all_cube_hier_views", "all_cube_hier_view_columns", "all_cube_measures", "all_cube_views",
            "all_cube_view_columns", "all_db_links", "all_def_audit_opts", "all_dependencies", "all_dequeue_queues", "all_dimensions", "all_dim_attributes", "all_dim_child_of", "all_dim_hierarchies",
            "all_dim_join_key", "all_dim_levels", "all_dim_level_key", "all_directories", "all_editioning_views", "all_editioning_views_ae", "all_editioning_view_cols", "all_editioning_view_cols_ae",
            "all_editions", "all_edition_comments", "all_encrypted_columns", "all_errors", "all_errors_ae", "all_evaluation_contexts", "all_evaluation_context_tables", "all_evaluation_context_vars",
            "all_external_locations", "all_external_tables", "all_file_groups", "all_file_group_export_info", "all_file_group_files", "all_file_group_tables", "all_file_group_tablespaces",
            "all_file_group_versions", "all_histograms", "all_identifiers", "all_indexes", "all_indextypes", "all_indextype_arraytypes", "all_indextype_comments", "all_indextype_operators",
            "all_ind_columns", "all_ind_expressions", "all_ind_partitions", "all_ind_pending_stats", "all_ind_statistics", "all_ind_subpartitions", "all_internal_triggers", "all_int_dequeue_queues",
            "all_java_arguments", "all_java_classes", "all_java_compiler_options", "all_java_derivations", "all_java_fields", "all_java_implements", "all_java_inners", "all_java_layouts",
            "all_java_methods", "all_java_ncomps", "all_java_resolvers", "all_java_throws", "all_jobs", "all_join_ind_columns", "all_libraries", "all_lobs", "all_lob_partitions",
            "all_lob_subpartitions", "all_lob_templates", "all_log_groups", "all_log_group_columns", "all_measure_folders", "all_measure_folder_contents", "all_method_params", "all_method_results",
            "all_mining_models", "all_mining_model_attributes", "all_mining_model_settings", "all_mviews", "all_mview_aggregates", "all_mview_analysis", "all_mview_comments",
            "all_mview_detail_partition", "all_mview_detail_relations", "all_mview_detail_subpartition", "all_mview_joins", "all_mview_keys", "all_mview_logs", "all_mview_refresh_times",
            "all_nested_tables", "all_nested_table_cols", "all_objects", "all_objects_ae", "all_object_tables", "all_obj_colattrs", "all_olap2_aws", "all_opancillary", "all_oparguments",
            "all_opbindings", "all_operators", "all_operator_comments", "all_outlines", "all_outline_hints", "all_partial_drop_tabs", "all_part_col_statistics", "all_part_histograms",
            "all_part_indexes", "all_part_key_columns", "all_part_lobs", "all_part_tables", "all_pending_conv_tables", "all_plsql_object_settings", "all_policies", "all_policy_contexts",
            "all_policy_groups", "all_probe_objects", "all_procedures", "all_propagation", "all_published_columns", "all_queues", "all_queue_publishers", "all_queue_schedules",
            "all_queue_subscribers", "all_queue_tables", "all_refresh", "all_refresh_children", "all_refresh_dependencies", "all_refs", "all_registered_mviews", "all_registered_snapshots",
            "all_registry_banners", "all_repaudit_attribute", "all_repaudit_column", "all_repcat", "all_repcatlog", "all_repcolumn", "all_repcolumn_group", "all_repconflict", "all_repddl",
            "all_repflavors", "all_repflavor_columns", "all_repflavor_objects", "all_repgenerated", "all_repgenobjects", "all_repgroup", "all_repgrouped_column", "all_repgroup_privileges",
            "all_repkey_columns", "all_repobject", "all_repparameter_column", "all_reppriority", "all_reppriority_group", "all_repprop", "all_represolution", "all_represolution_method",
            "all_represolution_statistics", "all_represol_stats_control", "all_repschema", "all_repsites", "all_rewrite_equivalences", "all_rules", "all_rulesets", "all_rule_sets",
            "all_rule_set_rules", "all_scheduler_chains", "all_scheduler_chain_rules", "all_scheduler_chain_steps", "all_scheduler_credentials", "all_scheduler_db_dests", "all_scheduler_dests",
            "all_scheduler_external_dests", "all_scheduler_file_watchers", "all_scheduler_global_attribute", "all_scheduler_groups", "all_scheduler_group_members", "all_scheduler_jobs",
            "all_scheduler_job_args", "all_scheduler_job_classes", "all_scheduler_job_dests", "all_scheduler_job_log", "all_scheduler_job_run_details", "all_scheduler_notifications",
            "all_scheduler_programs", "all_scheduler_program_args", "all_scheduler_remote_databases", "all_scheduler_remote_jobstate", "all_scheduler_running_chains", "all_scheduler_schedules",
            "all_scheduler_windows", "all_scheduler_window_details", "all_scheduler_window_groups", "all_scheduler_window_log", "all_scheduler_wingroup_members", "all_secondary_objects",
            "all_sec_relevant_cols", "all_sequences", "all_services", "all_snapshots", "all_snapshot_logs", "all_snapshot_refresh_times", "all_source", "all_source_ae", "all_source_tables",
            "all_sqlj_types", "all_sqlj_type_attrs", "all_sqlj_type_methods", "all_sqlset", "all_sqlset_binds", "all_sqlset_plans", "all_sqlset_references", "all_sqlset_statements",
            "all_stat_extensions", "all_stored_settings", "all_streams_columns", "all_streams_global_rules", "all_streams_message_consumers", "all_streams_message_rules",
            "all_streams_newly_supported", "all_streams_rules", "all_streams_schema_rules", "all_streams_table_rules", "all_streams_transform_function", "all_streams_unsupported",
            "all_subpartition_templates", "all_subpart_col_statistics", "all_subpart_histograms", "all_subpart_key_columns", "all_subscribed_columns", "all_subscribed_tables", "all_subscriptions",
            "all_sumdelta", "all_summap", "all_summaries", "all_sync_capture", "all_sync_capture_prepared_tabs", "all_sync_capture_tables", "all_synonyms", "all_tables", "all_tab_cols",
            "all_tab_columns", "all_tab_col_statistics", "all_tab_comments", "all_tab_histgrm_pending_stats", "all_tab_histograms", "all_tab_modifications", "all_tab_partitions",
            "all_tab_pending_stats", "all_tab_privs", "all_tab_privs_made", "all_tab_privs_recd", "all_tab_statistics", "all_tab_stats_history", "all_tab_stat_prefs", "all_tab_subpartitions",
            "all_transformations", "all_triggers", "all_trigger_cols", "all_trigger_ordering", "all_tstz_tables", "all_tstz_tab_cols", "all_types", "all_type_attrs", "all_type_methods",
            "all_type_versions", "all_unused_col_tabs", "all_updatable_columns", "all_users", "all_ustats", "all_varrays", "all_views", "all_views_ae", "all_warning_settings",
            "all_xds_attribute_secs", "all_xds_instance_sets", "all_xds_objects", "all_xml_indexes", "all_xml_schemas", "all_xml_schemas2", "all_xml_tables", "all_xml_tab_cols", "all_xml_views",
            "all_xml_view_cols", "all_xsc_aggregate_privilege", "all_xsc_privilege", "all_xsc_security_class", "all_xsc_security_class_dep", "all_xsc_security_class_status", "all_xstream_inbound",
            "all_xstream_outbound", "all_xstream_rules", "dba_2pc_neighbors", "dba_2pc_pending", "dba_addm_fdg_breakdown", "dba_addm_findings", "dba_addm_instances", "dba_addm_system_directives",
            "dba_addm_tasks", "dba_addm_task_directives", "dba_advisor_actions", "dba_advisor_commands", "dba_advisor_definitions", "dba_advisor_def_parameters", "dba_advisor_dir_definitions",
            "dba_advisor_dir_instances", "dba_advisor_dir_task_inst", "dba_advisor_executions", "dba_advisor_execution_types", "dba_advisor_exec_parameters", "dba_advisor_fdg_breakdown",
            "dba_advisor_findings", "dba_advisor_finding_names", "dba_advisor_journal", "dba_advisor_log", "dba_advisor_objects", "dba_advisor_object_types", "dba_advisor_parameters",
            "dba_advisor_parameters_proj", "dba_advisor_rationale", "dba_advisor_recommendations", "dba_advisor_sqla_colvol", "dba_advisor_sqla_rec_sum", "dba_advisor_sqla_tables",
            "dba_advisor_sqla_tabvol", "dba_advisor_sqla_wk_map", "dba_advisor_sqla_wk_stmts", "dba_advisor_sqla_wk_sum", "dba_advisor_sqlplans", "dba_advisor_sqlstats", "dba_advisor_sqlw_colvol",
            "dba_advisor_sqlw_journal", "dba_advisor_sqlw_parameters", "dba_advisor_sqlw_stmts", "dba_advisor_sqlw_sum", "dba_advisor_sqlw_tables", "dba_advisor_sqlw_tabvol",
            "dba_advisor_sqlw_templates", "dba_advisor_tasks", "dba_advisor_templates", "dba_advisor_usage", "dba_alert_arguments", "dba_alert_history", "dba_all_tables", "dba_analyze_objects",
            "dba_apply", "dba_apply_change_handlers", "dba_apply_conflict_columns", "dba_apply_dml_handlers", "dba_apply_enqueue", "dba_apply_error", "dba_apply_execute",
            "dba_apply_instantiated_global", "dba_apply_instantiated_objects", "dba_apply_instantiated_schemas", "dba_apply_key_columns", "dba_apply_object_dependencies", "dba_apply_parameters",
            "dba_apply_progress", "dba_apply_spill_txn", "dba_apply_table_columns", "dba_apply_value_dependencies", "dba_aq_agents", "dba_aq_agent_privs", "dba_arguments", "dba_assemblies",
            "dba_associations", "dba_attribute_transformations", "dba_audit_exists", "dba_audit_mgmt_cleanup_jobs", "dba_audit_mgmt_clean_events", "dba_audit_mgmt_config_params",
            "dba_audit_mgmt_last_arch_ts", "dba_audit_object", "dba_audit_policies", "dba_audit_policy_columns", "dba_audit_session", "dba_audit_statement", "dba_audit_trail", "dba_autotask_client",
            "dba_autotask_client_history", "dba_autotask_client_job", "dba_autotask_job_history", "dba_autotask_operation", "dba_autotask_schedule", "dba_autotask_task", "dba_autotask_window_clients",
            "dba_autotask_window_history", "dba_aws", "dba_aw_obj", "dba_aw_prop", "dba_aw_ps", "dba_base_table_mviews", "dba_blockers", "dba_capture", "dba_capture_extra_attributes",
            "dba_capture_parameters", "dba_capture_prepared_database", "dba_capture_prepared_schemas", "dba_capture_prepared_tables", "dba_catalog", "dba_change_notification_regs", "dba_clusters",
            "dba_cluster_hash_expressions", "dba_clu_columns", "dba_coll_types", "dba_col_comments", "dba_col_pending_stats", "dba_col_privs", "dba_common_audit_trail", "dba_comparison",
            "dba_comparison_columns", "dba_comparison_row_dif", "dba_comparison_scan", "dba_comparison_scan_summary", "dba_comparison_scan_values", "dba_connect_role_grantees", "dba_constraints",
            "dba_cons_columns", "dba_cons_obj_columns", "dba_context", "dba_cpool_info", "dba_cpu_usage_statistics", "dba_cq_notification_queries", "dba_cubes", "dba_cube_attributes",
            "dba_cube_attr_visibility", "dba_cube_build_processes", "dba_cube_calculated_members", "dba_cube_dimensionality", "dba_cube_dimensions", "dba_cube_dim_levels", "dba_cube_dim_models",
            "dba_cube_dim_views", "dba_cube_dim_view_columns", "dba_cube_hierarchies", "dba_cube_hier_levels", "dba_cube_hier_views", "dba_cube_hier_view_columns", "dba_cube_measures",
            "dba_cube_views", "dba_cube_view_columns", "dba_datapump_jobs", "dba_datapump_sessions", "dba_data_files", "dba_dbfs_hs", "dba_dbfs_hs_commands", "dba_dbfs_hs_fixed_properties",
            "dba_dbfs_hs_properties", "dba_db_links", "dba_ddl_locks", "dba_dependencies", "dba_dimensions", "dba_dim_attributes", "dba_dim_child_of", "dba_dim_hierarchies", "dba_dim_join_key",
            "dba_dim_levels", "dba_dim_level_key", "dba_directories", "dba_dml_locks", "dba_dmt_free_space", "dba_dmt_used_extents", "dba_editioning_views", "dba_editioning_views_ae",
            "dba_editioning_view_cols", "dba_editioning_view_cols_ae", "dba_editions", "dba_edition_comments", "dba_enabled_aggregations", "dba_enabled_traces", "dba_encrypted_columns",
            "dba_epg_dad_authorization", "dba_errors", "dba_errors_ae", "dba_evaluation_contexts", "dba_evaluation_context_tables", "dba_evaluation_context_vars", "dba_export_objects",
            "dba_export_paths", "dba_exp_files", "dba_exp_objects", "dba_exp_version", "dba_extents", "dba_external_locations", "dba_external_tables", "dba_feature_usage_statistics",
            "dba_fga_audit_trail", "dba_file_groups", "dba_file_group_export_info", "dba_file_group_files", "dba_file_group_tables", "dba_file_group_tablespaces", "dba_file_group_versions",
            "dba_flashback_archive", "dba_flashback_archive_tables", "dba_flashback_archive_ts", "dba_flashback_txn_report", "dba_flashback_txn_state", "dba_free_space", "dba_free_space_coalesced",
            "dba_global_context", "dba_high_water_mark_statistics", "dba_histograms", "dba_hist_active_sess_history", "dba_hist_ash_snapshot", "dba_hist_baseline", "dba_hist_baseline_details",
            "dba_hist_baseline_metadata", "dba_hist_baseline_template", "dba_hist_bg_event_summary", "dba_hist_buffered_queues", "dba_hist_buffered_subscribers", "dba_hist_buffer_pool_stat",
            "dba_hist_cluster_intercon", "dba_hist_colored_sql", "dba_hist_comp_iostat", "dba_hist_cr_block_server", "dba_hist_current_block_server", "dba_hist_database_instance", "dba_hist_datafile",
            "dba_hist_db_cache_advice", "dba_hist_dispatcher", "dba_hist_dlm_misc", "dba_hist_dyn_remaster_stats", "dba_hist_enqueue_stat", "dba_hist_event_histogram", "dba_hist_event_name",
            "dba_hist_filemetric_history", "dba_hist_filestatxs", "dba_hist_ic_client_stats", "dba_hist_ic_device_stats", "dba_hist_instance_recovery", "dba_hist_inst_cache_transfer",
            "dba_hist_interconnect_pings", "dba_hist_iostat_detail", "dba_hist_iostat_filetype", "dba_hist_iostat_filetype_name", "dba_hist_iostat_function", "dba_hist_iostat_function_name",
            "dba_hist_java_pool_advice", "dba_hist_latch", "dba_hist_latch_children", "dba_hist_latch_misses_summary", "dba_hist_latch_name", "dba_hist_latch_parent", "dba_hist_librarycache",
            "dba_hist_log", "dba_hist_memory_resize_ops", "dba_hist_memory_target_advice", "dba_hist_mem_dynamic_comp", "dba_hist_metric_name", "dba_hist_mttr_target_advice", "dba_hist_mutex_sleep",
            "dba_hist_optimizer_env", "dba_hist_osstat", "dba_hist_osstat_name", "dba_hist_parameter", "dba_hist_parameter_name", "dba_hist_persistent_queues", "dba_hist_persistent_subs",
            "dba_hist_pgastat", "dba_hist_pga_target_advice", "dba_hist_plan_operation_name", "dba_hist_plan_option_name", "dba_hist_process_mem_summary", "dba_hist_resource_limit",
            "dba_hist_rowcache_summary", "dba_hist_rsrc_consumer_group", "dba_hist_rsrc_plan", "dba_hist_rule_set", "dba_hist_seg_stat", "dba_hist_seg_stat_obj", "dba_hist_service_name",
            "dba_hist_service_stat", "dba_hist_service_wait_class", "dba_hist_sessmetric_history", "dba_hist_sess_time_stats", "dba_hist_sga", "dba_hist_sgastat", "dba_hist_sga_target_advice",
            "dba_hist_shared_pool_advice", "dba_hist_shared_server_summary", "dba_hist_snapshot", "dba_hist_snap_error", "dba_hist_sqlbind", "dba_hist_sqlcommand_name", "dba_hist_sqlstat",
            "dba_hist_sqltext", "dba_hist_sql_bind_metadata", "dba_hist_sql_plan", "dba_hist_sql_summary", "dba_hist_sql_workarea_hstgrm", "dba_hist_stat_name", "dba_hist_streams_apply_sum",
            "dba_hist_streams_capture", "dba_hist_streams_pool_advice", "dba_hist_sysmetric_history", "dba_hist_sysmetric_summary", "dba_hist_sysstat", "dba_hist_system_event",
            "dba_hist_sys_time_model", "dba_hist_tablespace_stat", "dba_hist_tbspc_space_usage", "dba_hist_tempfile", "dba_hist_tempstatxs", "dba_hist_thread", "dba_hist_toplevelcall_name",
            "dba_hist_undostat", "dba_hist_waitclassmet_history", "dba_hist_waitstat", "dba_hist_wr_control", "dba_ias_constraint_exp", "dba_ias_gen_stmts", "dba_ias_gen_stmts_exp", "dba_ias_objects",
            "dba_ias_objects_base", "dba_ias_objects_exp", "dba_ias_postgen_stmts", "dba_ias_pregen_stmts", "dba_ias_sites", "dba_ias_templates", "dba_identifiers", "dba_indexes", "dba_indextypes",
            "dba_indextype_arraytypes", "dba_indextype_comments", "dba_indextype_operators", "dba_ind_columns", "dba_ind_expressions", "dba_ind_partitions", "dba_ind_pending_stats",
            "dba_ind_statistics", "dba_ind_subpartitions", "dba_internal_triggers", "dba_invalid_objects", "dba_java_arguments", "dba_java_classes", "dba_java_compiler_options",
            "dba_java_derivations", "dba_java_fields", "dba_java_implements", "dba_java_inners", "dba_java_layouts", "dba_java_methods", "dba_java_ncomps", "dba_java_policy", "dba_java_resolvers",
            "dba_java_throws", "dba_jobs", "dba_jobs_running", "dba_join_ind_columns", "dba_keepsizes", "dba_kgllock", "dba_libraries", "dba_lmt_free_space", "dba_lmt_used_extents", "dba_lobs",
            "dba_lob_partitions", "dba_lob_subpartitions", "dba_lob_templates", "dba_lock", "dba_locks", "dba_lock_internal", "dba_logmnr_log", "dba_logmnr_purged_log", "dba_logmnr_session",
            "dba_logstdby_eds_supported", "dba_logstdby_eds_tables", "dba_logstdby_events", "dba_logstdby_history", "dba_logstdby_log", "dba_logstdby_not_unique", "dba_logstdby_parameters",
            "dba_logstdby_progress", "dba_logstdby_skip", "dba_logstdby_skip_transaction", "dba_logstdby_unsupported", "dba_logstdby_unsupported_table", "dba_log_groups", "dba_log_group_columns",
            "dba_measure_folders", "dba_measure_folder_contents", "dba_method_params", "dba_method_results", "dba_mining_models", "dba_mining_model_attributes", "dba_mining_model_settings",
            "dba_mining_model_tables", "dba_mviews", "dba_mview_aggregates", "dba_mview_analysis", "dba_mview_comments", "dba_mview_detail_partition", "dba_mview_detail_relations",
            "dba_mview_detail_subpartition", "dba_mview_joins", "dba_mview_keys", "dba_mview_logs", "dba_mview_log_filter_cols", "dba_mview_refresh_times", "dba_nested_tables",
            "dba_nested_table_cols", "dba_network_acls", "dba_network_acl_privileges", "dba_objects", "dba_objects_ae", "dba_object_size", "dba_object_tables", "dba_obj_audit_opts",
            "dba_obj_colattrs", "dba_oldimage_columns", "dba_opancillary", "dba_oparguments", "dba_opbindings", "dba_operators", "dba_operator_comments", "dba_optstat_operations", "dba_outlines",
            "dba_outline_hints", "dba_outstanding_alerts", "dba_parallel_execute_chunks", "dba_parallel_execute_tasks", "dba_partial_drop_tabs", "dba_part_col_statistics", "dba_part_histograms",
            "dba_part_indexes", "dba_part_key_columns", "dba_part_lobs", "dba_part_tables", "dba_pending_conv_tables", "dba_pending_transactions", "dba_plsql_object_settings", "dba_policies",
            "dba_policy_contexts", "dba_policy_groups", "dba_priv_audit_opts", "dba_procedures", "dba_profiles", "dba_propagation", "dba_proxies", "dba_published_columns", "dba_queues",
            "dba_queue_publishers", "dba_queue_schedules", "dba_queue_subscribers", "dba_queue_tables", "dba_rchild", "dba_recoverable_script", "dba_recoverable_script_blocks",
            "dba_recoverable_script_errors", "dba_recoverable_script_hist", "dba_recoverable_script_params", "dba_recyclebin", "dba_refresh", "dba_refresh_children", "dba_refs",
            "dba_registered_archived_log", "dba_registered_mviews", "dba_registered_mview_groups", "dba_registered_snapshots", "dba_registered_snapshot_groups", "dba_registry",
            "dba_registry_database", "dba_registry_dependencies", "dba_registry_hierarchy", "dba_registry_history", "dba_registry_log", "dba_registry_progress", "dba_repaudit_attribute",
            "dba_repaudit_column", "dba_repcat", "dba_repcatlog", "dba_repcat_exceptions", "dba_repcolumn", "dba_repcolumn_group", "dba_repconflict", "dba_repddl", "dba_repextensions",
            "dba_repflavors", "dba_repflavor_columns", "dba_repflavor_objects", "dba_repgenerated", "dba_repgenobjects", "dba_repgroup", "dba_repgrouped_column", "dba_repgroup_privileges",
            "dba_repkey_columns", "dba_repobject", "dba_repparameter_column", "dba_reppriority", "dba_reppriority_group", "dba_repprop", "dba_represolution", "dba_represolution_method",
            "dba_represolution_statistics", "dba_represol_stats_control", "dba_repschema", "dba_repsites", "dba_repsites_new", "dba_resource_incarnations", "dba_resumable", "dba_rewrite_equivalences",
            "dba_rgroup", "dba_roles", "dba_role_privs", "dba_rollback_segs", "dba_rsrc_capability", "dba_rsrc_categories", "dba_rsrc_consumer_groups", "dba_rsrc_consumer_group_privs",
            "dba_rsrc_group_mappings", "dba_rsrc_instance_capability", "dba_rsrc_io_calibrate", "dba_rsrc_manager_system_privs", "dba_rsrc_mapping_priority", "dba_rsrc_plans",
            "dba_rsrc_plan_directives", "dba_rsrc_storage_pool_mapping", "dba_rules", "dba_rulesets", "dba_rule_sets", "dba_rule_set_rules", "dba_scheduler_chains", "dba_scheduler_chain_rules",
            "dba_scheduler_chain_steps", "dba_scheduler_credentials", "dba_scheduler_db_dests", "dba_scheduler_dests", "dba_scheduler_external_dests", "dba_scheduler_file_watchers",
            "dba_scheduler_global_attribute", "dba_scheduler_groups", "dba_scheduler_group_members", "dba_scheduler_jobs", "dba_scheduler_job_args", "dba_scheduler_job_classes",
            "dba_scheduler_job_dests", "dba_scheduler_job_log", "dba_scheduler_job_roles", "dba_scheduler_job_run_details", "dba_scheduler_notifications", "dba_scheduler_programs",
            "dba_scheduler_program_args", "dba_scheduler_remote_databases", "dba_scheduler_remote_jobstate", "dba_scheduler_running_chains", "dba_scheduler_schedules", "dba_scheduler_windows",
            "dba_scheduler_window_details", "dba_scheduler_window_groups", "dba_scheduler_window_log", "dba_scheduler_wingroup_members", "dba_secondary_objects", "dba_sec_relevant_cols",
            "dba_segments", "dba_segments_old", "dba_sequences", "dba_server_registry", "dba_snapshots", "dba_snapshot_logs", "dba_snapshot_log_filter_cols", "dba_snapshot_refresh_times",
            "dba_source", "dba_source_ae", "dba_source_tables", "dba_sqlj_types", "dba_sqlj_type_attrs", "dba_sqlj_type_methods", "dba_sqlset", "dba_sqlset_binds", "dba_sqlset_definitions",
            "dba_sqlset_plans", "dba_sqlset_references", "dba_sqlset_statements", "dba_sqltune_binds", "dba_sqltune_plans", "dba_sqltune_rationale_plan", "dba_sqltune_statistics",
            "dba_sql_monitor_usage", "dba_sql_patches", "dba_sql_plan_baselines", "dba_sql_profiles", "dba_sscr_capture", "dba_sscr_restore", "dba_stat_extensions", "dba_stmt_audit_opts",
            "dba_stored_settings", "dba_streams_add_column", "dba_streams_administrator", "dba_streams_columns", "dba_streams_delete_column", "dba_streams_global_rules", "dba_streams_keep_columns",
            "dba_streams_message_consumers", "dba_streams_message_rules", "dba_streams_newly_supported", "dba_streams_rename_column", "dba_streams_rename_schema", "dba_streams_rename_table",
            "dba_streams_rules", "dba_streams_schema_rules", "dba_streams_split_merge", "dba_streams_split_merge_hist", "dba_streams_table_rules", "dba_streams_tp_component",
            "dba_streams_tp_component_link", "dba_streams_tp_component_stat", "dba_streams_tp_database", "dba_streams_tp_path_bottleneck", "dba_streams_tp_path_stat", "dba_streams_transformations",
            "dba_streams_transform_function", "dba_streams_unsupported", "dba_subpartition_templates", "dba_subpart_col_statistics", "dba_subpart_histograms", "dba_subpart_key_columns",
            "dba_subscribed_columns", "dba_subscribed_tables", "dba_subscriptions", "dba_subscr_registrations", "dba_summaries", "dba_sync_capture", "dba_sync_capture_prepared_tabs",
            "dba_sync_capture_tables", "dba_synonyms", "dba_sys_privs", "dba_tables", "dba_tablespaces", "dba_tablespace_groups", "dba_tablespace_thresholds", "dba_tablespace_usage_metrics",
            "dba_tab_cols", "dba_tab_columns", "dba_tab_col_statistics", "dba_tab_comments", "dba_tab_histgrm_pending_stats", "dba_tab_histograms", "dba_tab_modifications", "dba_tab_partitions",
            "dba_tab_pending_stats", "dba_tab_privs", "dba_tab_statistics", "dba_tab_stats_history", "dba_tab_stat_prefs", "dba_tab_subpartitions", "dba_template_refgroups", "dba_template_targets",
            "dba_temp_files", "dba_temp_free_space", "dba_thresholds", "dba_transformations", "dba_triggers", "dba_trigger_cols", "dba_trigger_ordering", "dba_tsm_destination", "dba_tsm_history",
            "dba_tsm_source", "dba_tstz_tables", "dba_tstz_tab_cols", "dba_ts_quotas", "dba_tune_mview", "dba_types", "dba_type_attrs", "dba_type_methods", "dba_type_versions", "dba_undo_extents",
            "dba_unused_col_tabs", "dba_updatable_columns", "dba_users", "dba_users_with_defpwd", "dba_ustats", "dba_varrays", "dba_views", "dba_views_ae", "dba_waiters", "dba_wallet_acls",
            "dba_warning_settings", "dba_workload_captures", "dba_workload_connection_map", "dba_workload_filters", "dba_workload_replays", "dba_workload_replay_divergence",
            "dba_workload_replay_filter_set", "dba_xds_attribute_secs", "dba_xds_instance_sets", "dba_xds_objects", "dba_xml_indexes", "dba_xml_schemas", "dba_xml_tables", "dba_xml_tab_cols",
            "dba_xml_views", "dba_xml_view_cols", "dba_xstream_inbound", "dba_xstream_outbound", "dba_xstream_rules", "user_addm_fdg_breakdown", "user_addm_findings", "user_addm_instances",
            "user_addm_tasks", "user_addm_task_directives", "user_advisor_actions", "user_advisor_dir_task_inst", "user_advisor_executions", "user_advisor_exec_parameters",
            "user_advisor_fdg_breakdown", "user_advisor_findings", "user_advisor_journal", "user_advisor_log", "user_advisor_objects", "user_advisor_parameters", "user_advisor_rationale",
            "user_advisor_recommendations", "user_advisor_sqla_colvol", "user_advisor_sqla_rec_sum", "user_advisor_sqla_tables", "user_advisor_sqla_tabvol", "user_advisor_sqla_wk_map",
            "user_advisor_sqla_wk_stmts", "user_advisor_sqla_wk_sum", "user_advisor_sqlplans", "user_advisor_sqlstats", "user_advisor_sqlw_colvol", "user_advisor_sqlw_journal",
            "user_advisor_sqlw_parameters", "user_advisor_sqlw_stmts", "user_advisor_sqlw_sum", "user_advisor_sqlw_tables", "user_advisor_sqlw_tabvol", "user_advisor_sqlw_templates",
            "user_advisor_tasks", "user_advisor_templates", "user_all_tables", "user_aq_agent_privs", "user_arguments", "user_assemblies", "user_associations", "user_attribute_transformations",
            "user_audit_object", "user_audit_policies", "user_audit_policy_columns", "user_audit_session", "user_audit_statement", "user_audit_trail", "user_aws", "user_aw_obj", "user_aw_prop",
            "user_aw_ps", "user_base_table_mviews", "user_catalog", "user_change_notification_regs", "user_clusters", "user_cluster_hash_expressions", "user_clu_columns", "user_coll_types",
            "user_col_comments", "user_col_pending_stats", "user_col_privs", "user_col_privs_made", "user_col_privs_recd", "user_comparison", "user_comparison_columns", "user_comparison_row_dif",
            "user_comparison_scan", "user_comparison_scan_summary", "user_comparison_scan_values", "user_constraints", "user_cons_columns", "user_cons_obj_columns", "user_cq_notification_queries",
            "user_cubes", "user_cube_attributes", "user_cube_attr_visibility", "user_cube_build_processes", "user_cube_calculated_members", "user_cube_dimensionality", "user_cube_dimensions",
            "user_cube_dim_levels", "user_cube_dim_models", "user_cube_dim_views", "user_cube_dim_view_columns", "user_cube_hierarchies", "user_cube_hier_levels", "user_cube_hier_views",
            "user_cube_hier_view_columns", "user_cube_measures", "user_cube_views", "user_cube_view_columns", "user_datapump_jobs", "user_dbfs_hs", "user_dbfs_hs_commands", "user_dbfs_hs_files",
            "user_dbfs_hs_fixed_properties", "user_dbfs_hs_properties", "user_db_links", "user_dependencies", "user_dimensions", "user_dim_attributes", "user_dim_child_of", "user_dim_hierarchies",
            "user_dim_join_key", "user_dim_levels", "user_dim_level_key", "user_editioning_views", "user_editioning_views_ae", "user_editioning_view_cols", "user_editioning_view_cols_ae",
            "user_encrypted_columns", "user_epg_dad_authorization", "user_errors", "user_errors_ae", "user_evaluation_contexts", "user_evaluation_context_tables", "user_evaluation_context_vars",
            "user_extents", "user_external_locations", "user_external_tables", "user_file_groups", "user_file_group_export_info", "user_file_group_files", "user_file_group_tables",
            "user_file_group_tablespaces", "user_file_group_versions", "user_flashback_archive", "user_flashback_archive_tables", "user_flashback_txn_report", "user_flashback_txn_state",
            "user_free_space", "user_histograms", "user_identifiers", "user_indexes", "user_indextypes", "user_indextype_arraytypes", "user_indextype_comments", "user_indextype_operators",
            "user_ind_columns", "user_ind_expressions", "user_ind_partitions", "user_ind_pending_stats", "user_ind_statistics", "user_ind_subpartitions", "user_internal_triggers",
            "user_java_arguments", "user_java_classes", "user_java_compiler_options", "user_java_derivations", "user_java_fields", "user_java_implements", "user_java_inners", "user_java_layouts",
            "user_java_methods", "user_java_ncomps", "user_java_policy", "user_java_resolvers", "user_java_throws", "user_jobs", "user_join_ind_columns", "user_libraries", "user_lobs",
            "user_lob_partitions", "user_lob_subpartitions", "user_lob_templates", "user_log_groups", "user_log_group_columns", "user_measure_folders", "user_measure_folder_contents",
            "user_method_params", "user_method_results", "user_mining_models", "user_mining_model_attributes", "user_mining_model_settings", "user_mviews", "user_mview_aggregates",
            "user_mview_analysis", "user_mview_comments", "user_mview_detail_partition", "user_mview_detail_relations", "user_mview_detail_subpartition", "user_mview_joins", "user_mview_keys",
            "user_mview_logs", "user_mview_refresh_times", "user_nested_tables", "user_nested_table_cols", "user_network_acl_privileges", "user_objects", "user_objects_ae", "user_object_size",
            "user_object_tables", "user_obj_audit_opts", "user_obj_colattrs", "user_oldimage_columns", "user_opancillary", "user_oparguments", "user_opbindings", "user_operators",
            "user_operator_comments", "user_outlines", "user_outline_hints", "user_parallel_execute_chunks", "user_parallel_execute_tasks", "user_partial_drop_tabs", "user_part_col_statistics",
            "user_part_histograms", "user_part_indexes", "user_part_key_columns", "user_part_lobs", "user_part_tables", "user_password_limits", "user_pending_conv_tables",
            "user_plsql_object_settings", "user_policies", "user_policy_contexts", "user_policy_groups", "user_procedures", "user_proxies", "user_published_columns", "user_queues",
            "user_queue_publishers", "user_queue_schedules", "user_queue_subscribers", "user_queue_tables", "user_recyclebin", "user_refresh", "user_refresh_children", "user_refs",
            "user_registered_mviews", "user_registered_snapshots", "user_registry", "user_repaudit_attribute", "user_repaudit_column", "user_repcat", "user_repcatlog", "user_repcolumn",
            "user_repcolumn_group", "user_repconflict", "user_repddl", "user_repflavors", "user_repflavor_columns", "user_repflavor_objects", "user_repgenerated", "user_repgenobjects",
            "user_repgroup", "user_repgrouped_column", "user_repgroup_privileges", "user_repkey_columns", "user_repobject", "user_repparameter_column", "user_reppriority", "user_reppriority_group",
            "user_repprop", "user_represolution", "user_represolution_method", "user_represolution_statistics", "user_represol_stats_control", "user_repschema", "user_repsites",
            "user_resource_limits", "user_resumable", "user_rewrite_equivalences", "user_role_privs", "user_rsrc_consumer_group_privs", "user_rsrc_manager_system_privs", "user_rules", "user_rulesets",
            "user_rule_sets", "user_rule_set_rules", "user_scheduler_chains", "user_scheduler_chain_rules", "user_scheduler_chain_steps", "user_scheduler_credentials", "user_scheduler_db_dests",
            "user_scheduler_dests", "user_scheduler_file_watchers", "user_scheduler_groups", "user_scheduler_group_members", "user_scheduler_jobs", "user_scheduler_job_args",
            "user_scheduler_job_dests", "user_scheduler_job_log", "user_scheduler_job_run_details", "user_scheduler_notifications", "user_scheduler_programs", "user_scheduler_program_args",
            "user_scheduler_remote_jobstate", "user_scheduler_running_chains", "user_scheduler_schedules", "user_secondary_objects", "user_sec_relevant_cols", "user_segments", "user_sequences",
            "user_snapshots", "user_snapshot_logs", "user_snapshot_refresh_times", "user_source", "user_source_ae", "user_source_tables", "user_sqlj_types", "user_sqlj_type_attrs",
            "user_sqlj_type_methods", "user_sqlset", "user_sqlset_binds", "user_sqlset_definitions", "user_sqlset_plans", "user_sqlset_references", "user_sqlset_statements", "user_sqltune_binds",
            "user_sqltune_plans", "user_sqltune_rationale_plan", "user_sqltune_statistics", "user_stat_extensions", "user_stored_settings", "user_subpartition_templates",
            "user_subpart_col_statistics", "user_subpart_histograms", "user_subpart_key_columns", "user_subscribed_columns", "user_subscribed_tables", "user_subscriptions",
            "user_subscr_registrations", "user_summaries", "user_synonyms", "user_sys_privs", "user_tables", "user_tablespaces", "user_tab_cols", "user_tab_columns", "user_tab_col_statistics",
            "user_tab_comments", "user_tab_histgrm_pending_stats", "user_tab_histograms", "user_tab_modifications", "user_tab_partitions", "user_tab_pending_stats", "user_tab_privs",
            "user_tab_privs_made", "user_tab_privs_recd", "user_tab_statistics", "user_tab_stats_history", "user_tab_stat_prefs", "user_tab_subpartitions", "user_transformations", "user_triggers",
            "user_trigger_cols", "user_ts", "user_tstz_tables", "user_tstz_tab_cols", "user_ts_quotas", "user_tune_mview", "user_types", "user_type_attrs", "user_type_methods", "user_type_versions",
            "user_unused_col_tabs", "user_updatable_columns", "user_users", "user_ustats", "user_varrays", "user_views", "user_views_ae", "user_warning_settings", "user_xds_attribute_secs",
            "user_xds_instance_sets", "user_xds_objects", "user_xml_column_names", "user_xml_indexes", "user_xml_schemas", "user_xml_tables", "user_xml_tab_cols", "user_xml_views",
            "user_xml_view_cols"));
    
    static {
        List<String> resourceNames;
        try (Stream<String> resourceNameStream = ClasspathResourceDirectoryReader.read("schema")) {
            resourceNames = resourceNameStream.filter(each -> each.endsWith(".yaml")).collect(Collectors.toList());
        }
        DATABASE_TYPE_SCHEMA_TABLE_MAP = resourceNames.stream().map(resourceName -> resourceName.split("/")).filter(each -> each.length == 4)
                .collect(Collectors.groupingBy(path -> path[1], CaseInsensitiveMap::new, Collectors.groupingBy(path -> path[2], CaseInsensitiveMap::new,
                        Collectors.mapping(path -> StringUtils.removeEnd(path[3], ".yaml"), Collectors.toCollection(CaseInsensitiveSet::new)))));
        // SPEX ADDED: BEGIN
        Collection<String> oraclePublicTables = DATABASE_TYPE_SCHEMA_TABLE_MAP.computeIfAbsent("oracle", key1 -> new CaseInsensitiveMap<>())
                .computeIfAbsent("public", key -> new CaseInsensitiveSet<>());
        oraclePublicTables.addAll(ORACLE_PUBLIC);
        addSystemSchemaTableForOpenGauss();
        // SPEX ADDED: END
        DATABASE_TYPE_SCHEMA_RESOURCE_MAP = resourceNames.stream().map(resourceName -> resourceName.split("/")).filter(each -> each.length == 4)
                .collect(Collectors.groupingBy(path -> path[1], CaseInsensitiveMap::new, Collectors.groupingBy(path -> path[2], CaseInsensitiveMap::new,
                        Collectors.mapping(path -> String.join("/", path), Collectors.toCollection(CaseInsensitiveSet::new)))));
    }
    
    @SphereEx
    private static void addSystemSchemaTableForOpenGauss() {
        Collection<String> systemSchemas = Arrays.asList("pg_catalog", "information_schema", "pg_toast", "cstore", "pkg_service", "dbe_perf", "snapshot", "blockchain",
                "sqladvisor", "dbe_pldebugger", "dbe_pldeveloper", "dbe_sql_util", "information_schema", "db4ai");
        if (!DATABASE_TYPE_SCHEMA_TABLE_MAP.containsKey("openGauss")) {
            return;
        }
        Map<String, Collection<String>> systemTables = DATABASE_TYPE_SCHEMA_TABLE_MAP.get("openGauss");
        for (String each : systemSchemas) {
            if (!systemTables.containsKey(each)) {
                systemTables.put(each, Collections.emptyList());
            }
        }
    }
    
    /**
     * Judge whether current table is system table or not.
     *
     * @param schema schema
     * @param tableName table name
     * @return whether current table is system table or not
     */
    public static boolean isSystemTable(final String schema, final String tableName) {
        for (Entry<String, Map<String, Collection<String>>> entry : DATABASE_TYPE_SCHEMA_TABLE_MAP.entrySet()) {
            if (Optional.ofNullable(entry.getValue().get(schema)).map(tables -> tables.contains(tableName)).orElse(false)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Judge whether current table is system table or not.
     *
     * @param databaseType database type
     * @param schema schema
     * @param tableName table name
     * @return whether current table is system table or not
     */
    public static boolean isSystemTable(final String databaseType, final String schema, final String tableName) {
        Map<String, Collection<String>> schemaTableMap = DATABASE_TYPE_SCHEMA_TABLE_MAP.getOrDefault(databaseType, Collections.emptyMap());
        Map<String, Collection<String>> commonTableMap = DATABASE_TYPE_SCHEMA_TABLE_MAP.getOrDefault(COMMON, Collections.emptyMap());
        if (null == schema) {
            return schemaTableMap.values().stream().anyMatch(each -> each.contains(tableName)) || commonTableMap.values().stream().anyMatch(each -> each.contains(tableName));
        }
        return schemaTableMap.getOrDefault(schema, Collections.emptyList()).contains(tableName) || commonTableMap.getOrDefault(schema, Collections.emptyList()).contains(tableName);
    }
    
    /**
     * Judge whether current table is system table or not.
     *
     * @param databaseType database type
     * @param schema schema
     * @param tableNames table names
     * @return whether current table is system table or not
     */
    public static boolean isSystemTable(final String databaseType, final String schema, final Collection<String> tableNames) {
        Collection<String> databaseTypeTables = Optional.ofNullable(DATABASE_TYPE_SCHEMA_TABLE_MAP.get(databaseType)).map(schemas -> schemas.get(schema)).orElse(Collections.emptyList());
        Collection<String> commonTables = Optional.ofNullable(DATABASE_TYPE_SCHEMA_TABLE_MAP.get(COMMON)).map(schemas -> schemas.get(schema)).orElse(Collections.emptyList());
        for (String each : tableNames) {
            if (!databaseTypeTables.contains(each) && !commonTables.contains(each)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Get tables.
     *
     * @param databaseType database type
     * @param schema schema
     * @return optional tables
     */
    public static Collection<String> getTables(final String databaseType, final String schema) {
        Collection<String> result = new LinkedList<>();
        Optional.ofNullable(DATABASE_TYPE_SCHEMA_TABLE_MAP.get(databaseType)).map(schemas -> schemas.get(schema)).ifPresent(result::addAll);
        Optional.ofNullable(DATABASE_TYPE_SCHEMA_TABLE_MAP.get(COMMON)).map(schemas -> schemas.get(schema)).ifPresent(result::addAll);
        return result;
    }
    
    /**
     * Get all input streams.
     *
     * @param databaseType database type
     * @param schema schema
     * @return inputStream collection
     */
    public static Collection<InputStream> getAllInputStreams(final String databaseType, final String schema) {
        Collection<InputStream> result = new LinkedList<>();
        result.addAll(getInputStreams(databaseType, schema));
        result.addAll(getInputStreams(COMMON, schema));
        return result;
    }
    
    private static Collection<InputStream> getInputStreams(final String databaseType, final String schema) {
        if (!DATABASE_TYPE_SCHEMA_RESOURCE_MAP.containsKey(databaseType) || !DATABASE_TYPE_SCHEMA_RESOURCE_MAP.get(databaseType).containsKey(schema)) {
            return Collections.emptyList();
        }
        Collection<InputStream> result = new LinkedList<>();
        for (String each : DATABASE_TYPE_SCHEMA_RESOURCE_MAP.get(databaseType).get(schema)) {
            result.add(SystemSchemaManager.class.getClassLoader().getResourceAsStream(each));
        }
        return result;
    }
    
    /**
     * Is system schema.
     *
     * @param schema schema
     * @return true or false
     */
    @SphereEx
    public static boolean isSystemSchema(final String schema) {
        for (Entry<String, Map<String, Collection<String>>> entry : DATABASE_TYPE_SCHEMA_TABLE_MAP.entrySet()) {
            if (entry.getValue().containsKey(schema)) {
                return true;
            }
        }
        return false;
    }
}
