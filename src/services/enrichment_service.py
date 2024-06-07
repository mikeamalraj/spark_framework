from __future__ import annotations
from abc import ABC, abstractmethod

import sqlparse
from pyspark.sql import *
from pyspark.sql import functions as F
import pandas as pd
from .datasource_service import *
from src.utils.read_write_utils import read_df_from_source
from src.utils.helpers import *
from src.utils.enrichmentutil import get_rename_col_df


class EnrichmentConfig(ABC):
    session = None

    @abstractmethod
    def execute(self, df: DataFrame):
        pass

    @abstractmethod
    def execute(self, left_df: DataFrame, right_df: DataFrame):
        pass

    def execute(self, df: DataFrame, config_df: pd.DataFrame, param_dict: dict):
        pass


class ColumnEnrichment(EnrichmentConfig):

    def __init__(self, application_name, parameters: BaseParam, priority=1, env="LOCAL"):
        self.application_name = application_name
        self.parameters = parameters
        self.priority = priority
        self.env = env

    def __str__(self):
        return f"ColumnEnrichment stage: {self.priority}"

    def execute(self, df: DataFrame):
        pass

    def execute(self, df: DataFrame, ref_enrichment_df: pd.DataFrame, param_dict: dict):
        ref_filtered_df = ref_enrichment_df.query(f"priority=={self.priority}")
        drop_cols_df = ref_filtered_df.query("enrichment_type in ('Drop', 'Rename')")

        if not drop_cols_df.empty:
            drop_cols_df = drop_cols_df.groupby('priority', group_keys=True) \
                .apply(lambda x: ','.join(x.enrichment_rule)).reset_index(name='drop_cols')

        ref_filtered_dict = ref_filtered_df.query("enrichment_type not in ('Drop')").to_dict('records')

        if not drop_cols_df.empty:
            drop_cols_dict = drop_cols_df.to_dict('records')[0]['drop_cols']
            drop_col_list = get_list_from_string(drop_cols_dict)
        else:
            drop_col_list = []

        list_of_columns_in_df = get_lower_list(df.columns)

        list_columns_from_conf = [get_lower_str(item['target_col_name']) for item in ref_filtered_dict]

        list_of_rules = [(item['enrichment_rule'], get_lower_str(item['target_col_name'])) for item in
                         ref_filtered_dict]

        select_cols_df = ",".join([
            f"`{x}`" for x in list_of_columns_in_df if x not in (list_columns_from_conf + drop_col_list)
        ])

        case_statement = generate_enrich_statement(list_of_rules)
        if case_statement is not None and case_statement != '':
            case_statement = f", {case_statement} "

        spark_statement = f"SELECT {select_cols_df} {case_statement} FROM {TEMP_TABLE}"
        spark_statement = spark_statement.format_map(param_dict)

        if self.parameters.is_debug:
            spark_statement = sqlparse.format(spark_statement, reindent=True, keyword_case='upper')
            print(spark_statement)

        df.createOrReplaceTempView(TEMP_TABLE)
        enriched_df = df.sparkSession.sql(spark_statement)

        if self.parameters.is_debug:
            enriched_df.show(10, False)

        return enriched_df


class JoinConfig(EnrichmentConfig):

    def __init__(self, exec_sequence: int, right_table_config: DataConfig, left_join_columns: list[str],
                 right_join_columns: list[str], right_columns: list[str], prefix, join_type, parameters: BaseParam,
                 is_broadcast="N", is_distinct="N", execution_type: str = "DataFrame", additional_join_condition=None,
                 left_table_ref: str = None, right_table_ref: str = None, join_query: str = None):
        self.exec_sequence = exec_sequence
        self.right_table_config = right_table_config
        self.left_join_columns = left_join_columns
        self.right_join_columns = right_join_columns
        self.right_columns = right_columns
        self.prefix = prefix
        self.join_type = join_type
        self.parameters = parameters
        self.broadcast_flag = is_broadcast
        self.distinct_flag = is_distinct
        self.execution_type = execution_type
        self.additional_join_condition = additional_join_condition
        self.left_table_ref = left_table_ref
        self.right_table_ref = right_table_ref
        self.join_query = join_query

    def __str__(self):
        return f"right_table_config: {self.right_table_config}, left_join_columns: {self.left_join_columns}, " \
               f"right_join_columns: {self.right_join_columns}, right_columns: {self.right_columns}, prefix: {self.prefix}"

    def execute(self, df: DataFrame, right_df: DataFrame = None, param_dict: dict = {}):

        if right_df is None and self.right_table_config is None:
            raise Exception("Invalid join configuration! right_ds is incorrect!")

        if right_df is None:
            right_df: DataFrame = read_df_from_source(self.session, self.right_table_config,
                                                      self.parameters.environment,
                                                      self.parameters.skip_local, param_dict)

        if self.execution_type == "QUERY":
            df.createOrReplaceTempView(self.left_table_ref)
            right_df.createOrReplaceTempView(self.right_table_ref)
            result_df = df.sparkSession.sql(self.join_query.format(param_dict))
        else:
            required_cols = self.right_columns + self.right_join_columns

            if '*' in required_cols:
                if self.distinct_flag == "Y":
                    right_df = right_df.dropDuplicates()
                else:
                    right_df = right_df.select(required_cols)
                    if self.distinct_flag == "Y":
                        right_df = right_df.dropDuplicates(required_cols)
            right_join_column = self.right_join_columns

            if self.prefix is not None and self.prefix != "":
                # rename columns in right join condition
                right_join_column = [self.prefix + "_" + x for x in self.right_join_columns]

                # rename columns in right dataframe
                rename_col_list = [(col_name, self.prefix + "_" + col_name) for col_name in right_df.columns]

                right_df = get_rename_col_df(right_df, rename_col_list)

            # create join expression and apply join
            if self.broadcast_flag == "Y":
                right_df = F.broadcast(right_df)

            if self.left_join_columns == right_join_column:
                join_condition = self.left_join_columns
            else:
                join_condition = [F.col(x).eqNullSafe(F.col(y)) for (x, y) in
                                  zip(self.left_join_columns, right_join_column)]

            if self.additional_join_condition is not None and self.additional_join_condition != "":
                join_condition.append(F.expr(self.additional_join_condition.format_map(param_dict)))

            if self.left_table_ref is not None and self.left_table_ref != "":
                df = df.alias(self.left_table_ref)

            if self.right_table_ref is not None and self.right_table_ref != "":
                right_df = right_df.alias(self.right_table_ref)

            result_df = df.join(right_df, join_condition, self.join_type)

        if self.parameters.is_debug:
            result_df.show(10, False)

        return result_df


def generate_enrich_statement(lst):
    query = ""
    list_of_numbers = list(range(len(lst)))
    k = list(zip(lst, list_of_numbers))
    for (x, y) in k:
        if y < len(lst) - 1:
            query += x[0] + " as " + x[1] + "," + "\n"
        else:
            query += x[0] + " as " + x[1] + ","
    return query
