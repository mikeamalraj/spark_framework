ENV_PARAM = {'env': None}
JOB_PARAM = {'stress_model': None, 'job_name': None}
DATASET_PARAM = {'stress_model': None, 'list_of_ds_names': None}

JOB_CONFIG_TBL = "cf_cmp_job_config"
JOB_CONFIG_QUERY = f"select * from {JOB_CONFIG_TBL} where lower(stress_test_model)=%(stress_model)s and job_name=%(" \
                   f"job_name)s"

DATASET_CONFIG_TBL = "cf_cmp_dataset_config"
DATASET_CONFIG_QUERY = f"select * from {DATASET_CONFIG_TBL} where lower(stress_test_model)=%(stress_model)s and " \
                       f"lower(trim(ds_name)) in %(list_of_ds_names)s"

JOIN_CONFIG_TBL = "cf_cmp_join_config"
JOIN_CONFIG_QUERY = f"select * from {JOIN_CONFIG_TBL} where lower(stress_test_model)=%(stress_model)s and job_name=%(" \
                    f"job_name)s and is_active='Y' order by priority, exec_sequence"

ENRICH_CONF_TBL = "cf_cmp_enrichment_rules"
ENRICH_CONF_QUERY = f"select * from {ENRICH_CONF_TBL} where lower(stress_test_model)=%(stress_model)s and job_name=%(" \
                    f"job_name)s and is_active='Y'"

RESULT_CONFIG_TBL = "cf_cmp_result_table_config"
RESULT_CONFIG_QUERY = f"select * from {RESULT_CONFIG_TBL} where lower(stress_test_model) =%(stress_model)s and " \
                      f"job_name=%(job_name)s"
