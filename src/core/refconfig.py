from src.constants.sqlconstant import *
from src.core.params import Param
from src.services.enrichment_service import *
from src.utils.helpers import *
from src.constants.lib_constants import LOWER
from src.utils.talk_to_db import TalkToDb
import pprint


class BaseRefConfig:

    def __init__(self):
        self.db_service: TalkToDb = None
        self.param = None
        self.ENV_PARAM = dict()
        self.JOB_PARAM = dict()
        self.DATASET_PARAM = dict()
        self.env_configs = dict()
        self.job_config_dict = dict()
        self.dataset_config_dict = dict()
        self.join_conf_dict_obj = dict()
        self.enrich_conf_dict_obj = dict()
        self.result_tables_dict_obj = dict()
        self.base_dataset_conf = None
        self.target_dataset_conf = None
        self.enrich_conf_df = None

    def init_configurations(self, param: BaseParam, app_name: str, env: str):
        self.db_service = TalkToDb(env)
        self.param = param
        self.ENV_PARAM = ENV_PARAM
        self.ENV_PARAM["env"] = env
        self.JOB_PARAM = JOB_PARAM
        self.JOB_PARAM["stress_model"] = param.stress_model.lower()
        self.JOB_PARAM["job_name"] = app_name.lower()
        self.DATASET_PARAM = DATASET_PARAM
        self.DATASET_PARAM["stress_model"] = param.stress_model.lower()
        self.set_jobs_config()
        self.set_dataset_config()
        self.set_join_config()
        self.set_enrich_config()
        self.set_base_dataset_config()
        self.set_target_dataset_config()
        self.set_result_tables_config()

    def set_jobs_config(self):
        jobs_conf_df = self.db_service.read_from_db(JOB_CONFIG_QUERY, params=self.JOB_PARAM)

        if not jobs_conf_df.empty:
            job_conf_dict = jobs_conf_df.to_dict("records")[0]
            list_of_ds_name = get_list_from_string(job_conf_dict['ref_df'], LOWER)
            list_of_ds_name.append(job_conf_dict['ds_name'])
            self.DATASET_PARAM['list_of_ds_names'] = list_of_ds_name
            self.job_config_dict = job_conf_dict
        else:
            raise Exception("Job configuration is empty! Please check job name/conf!")

    def set_dataset_config(self):
        dataset_conf_df = self.db_service.read_from_db(DATASET_CONFIG_QUERY, params=self.DATASET_PARAM)
        dataset_conf_dict = {}

        for conf in dataset_conf_df.to_dict('records'):
            obj = None
            if conf['source_of_df'].upper() == "HIVE":
                obj = SourceHiveConfig(conf['name_of_schema'], conf['table_name'], conf['partition_filter_condition'],
                                       conf['filter_condition'], conf['filter_max_version_key'],
                                       get_list_from_string(conf['select_cols'], LOWER),
                                       get_upper_str(conf['is_query']), conf['query'], self.param)
            elif conf['source_of_df'].upper() == "MARIADB":
                obj = MariaDBConfig(conf['name_of_schema'], conf['table_name'], conf['partition_filter_condition'],
                                    conf['filter_condition'], conf['filter_max_version_key'],
                                    get_list_from_string(conf['select_cols'], LOWER),
                                    get_upper_str(conf['is_query']), conf['query'], self.param)
            elif conf['source_of_df'].upper() == "HDFS":
                obj = SourceHadoopConfig(conf['src_path'], conf['partition_filter_condition'], conf['filter_condition'],
                                         conf['filter_max_version_key'],
                                         get_list_from_string(conf['select_cols'], LOWER), self.param,
                                         get_lower_str(conf['file_format']), conf['delimiter'])
            dataset_conf_dict[conf['ds_name']] = obj

        self.dataset_config_dict = dataset_conf_dict

    def set_join_config(self):
        join_conf_df = self.db_service.read_from_db(JOIN_CONFIG_QUERY, params=self.JOB_PARAM)

        join_conf_dict = join_conf_df.to_dict('records')
        join_conf_dict_obj = {}

        for join_conf in join_conf_dict:
            join_conf_dict_obj[join_conf['priority']] = []

        for join_conf in join_conf_dict:
            if join_conf['right_ds'] in self.dataset_config_dict:
                dataset_obj = self.dataset_config_dict[join_conf['right_ds']]
            else:
                dataset_obj = None

            join_obj = JoinConfig(join_conf['exec_sequence'], dataset_obj,
                                  get_list_from_string(join_conf['left_join_cols'], LOWER),
                                  get_list_from_string(join_conf['right_join_cols'], LOWER),
                                  get_list_from_string(join_conf['right_cols'], LOWER),
                                  join_conf['join_prefix'], join_conf['join_type'],
                                  self.param, get_upper_str(join_conf['is_broadcast_flag']),
                                  get_upper_str(join_conf['is_distinct_flag']), get_upper_str(join_conf['exec_type']),
                                  join_conf['additional_join_config'], join_conf['left_table_ref'],
                                  join_conf['right_table_ref'], join_conf['join_query']
                                  )

            join_conf_dict_obj[join_conf['priority']].append(join_obj)

        self.join_conf_dict_obj = join_conf_dict_obj

    def set_enrich_config(self):
        enrich_conf_df = self.db_service.read_from_db(ENRICH_CONF_QUERY, params=self.JOB_PARAM)

        enrich_conf_dict = enrich_conf_df[['job_name', 'priority']].drop_duplicates().to_dict('records')
        enrich_conf_dict_obj = {}
        for conf in enrich_conf_dict:
            enrich_obj = ColumnEnrichment(JOB_PARAM['job_name'], self.param, conf['priority'], self.param.environment)
            enrich_conf_dict_obj.update({conf['priority']: enrich_obj})
        self.enrich_conf_dict_obj = enrich_conf_dict_obj
        self.enrich_conf_df = enrich_conf_df

    def set_base_dataset_config(self):
        if self.job_config_dict['ds_name'] is not None:
            source_data_conf = self.dataset_config_dict[self.job_config_dict['ds_name']]
            self.base_dataset_conf = source_data_conf

    def set_target_dataset_config(self):
        self.target_dataset_conf = self.get_target_config_obj(self.job_config_dict)

    def set_result_tables_config(self):
        results_conf_df = self.db_service.read_from_db(RESULT_CONFIG_QUERY, params=self.JOB_PARAM)

        results_tbl_conf_dict = {}
        for conf in results_conf_df.to_dict('records'):
            obj = self.get_target_config_obj(conf)
            results_tbl_conf_dict.update({conf['ds_name']: obj})

        self.result_tables_dict_obj = results_tbl_conf_dict

    def get_target_config_obj(self, conf: dict):
        if conf['target_type'].upper() == "HIVE":
            target_obj = TargetHiveConfig(conf['target_path'], conf['target_schema'], conf['target_table_name'],
                                          get_list_from_string(conf['target_partition_cols'], LOWER),
                                          get_list_from_string(conf['target_cols'], LOWER),
                                          conf['write_mode'], conf['file_format'])
        elif conf['target_type'].upper() == "HDFS":
            target_obj = TargetHadoopConfig(
                conf['target_path'], conf['target_table_name'],
                get_list_from_string(conf['target_partition_cols'], LOWER),
                get_list_from_string(conf['target_cols'], LOWER),
                conf['write_mode'], conf['file_format']
            )
        elif conf['target_type'].upper() == "MARIADB":
            target_obj = TargetMariaDBConfig(
                conf['target_schema'], conf['target_table_name'],
                get_list_from_string(conf['target_partition_cols'], LOWER),
                get_list_from_string(conf['target_cols'], LOWER),
                conf['write_mode'], conf['overwrite_condition']
            )
        else:
            target_obj = None

        return target_obj

    def display_configurations(self):
        print("********************** CONFIGS **************")
        print("ENV_PARAM", end="")
        pprint.pprint(self.ENV_PARAM)

        print("JOB_PARAM", end="")
        pprint.pprint(self.JOB_PARAM)

        print("DATASET_PARAM", end="")
        pprint.pprint(self.DATASET_PARAM)

        print("job_config_dict", end="")
        pprint.pprint(self.job_config_dict)

        print("dataset_config_dict", end="")
        pprint.pprint(self.dataset_config_dict)

        print("join_conf_dict_obj", end="")
        pprint.pprint(self.join_conf_dict_obj)

        print("enrich_conf_dict_obj", end="")
        pprint.pprint(self.enrich_conf_dict_obj)

        print("result_tables_dict_obj", end="")
        pprint.pprint(self.result_tables_dict_obj)

        print("base_dataset_conf", end="")
        pprint.pprint(self.base_dataset_conf)

        print("target_dataset_conf", end="")
        pprint.pprint(self.target_dataset_conf)


if __name__ == "__main__":
    from src.utils.env import set_environment_var

    set_environment_var(None, "LOCAL")

    bs_obj = BaseRefConfig()
    cmd_args = "local sample 10 20 30".split()
    param = Param(cmd_args, "sample")
    print(param)

    bs_obj.init_configurations(param, "sample", "LOCAL")
    bs_obj.display_configurations()
