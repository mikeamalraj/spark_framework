from __future__ import annotations

import sqlparse

from src.services.enrichment_service import generate_enrich_statement
from pyspark.sql import DataFrame
import pandas as pd
from src.utils.helpers import get_lower_str, get_lower_list


def get_joined_df(df: DataFrame, join_conf_dict_obj, start: int, end: int, right_df: DataFrame = None,
                  param_dict: dict = None):
    for i in range(start, end + 1):
        print(f"Join enrichment start from stage {i}")
        for enrichment in join_conf_dict_obj[i]:
            print(f"Join stage: {i}, exec_sequence: {enrichment.exec_sequence}, " + enrichment.__str__())
            enrichment.session = df.sparkSession
            if right_df is not None:
                df = enrichment.execute(df, right_df=right_df, param_dict=param_dict)
            else:
                df = enrichment.execute(df, param_dict=param_dict)

        print(f"Join enrichment finished for stage: {i}")
    return df


def enrich_df_in_sequence(df: DataFrame, enrich_conf_dict_obj, enrich_conf_df: pd.DataFrame, start: int, end: int,
                          param_dict: dict = None):
    for i in range(start, end + 1):
        if i in enrich_conf_dict_obj:
            print(f"Column enrichment started for stage: {i}")
            df = enrich_conf_dict_obj[i].execute(df, enrich_conf_df, param_dict=param_dict)
    return df


def enrich_df_using_cte(df: DataFrame, enrich_conf_df: pd.DataFrame, start: int, end: int,
                        param_dict: dict = None) -> DataFrame:
    print(f"Enriching Dataframe => start stage: {start}, end stage: {end}")
    final_query = ""
    for i in range(start, end + 1):
        ref_filter_dict = enrich_conf_df.query(f"priority=={i}").to_dict('records')
        list_of_column_in_df = get_lower_list(df.columns)
        list_of_rules = [(item['enrichment_rule'], get_lower_list(item['target_col_name'])) for item in ref_filter_dict]
        select_cols_df = ",".join([f"`{x}`" for x in list_of_column_in_df])

        case_statement = generate_enrich_statement(list_of_rules)
        if case_statement is not None and case_statement != "":
            case_statement = f", {case_statement}"

        temp_table_name = "TEMP_TABLE" if i == start else f"TEMP_TABLE_{i - 1}"
        if i != start:
            select_cols_df = "*"
        sql_statement = f"SELECT {select_cols_df} {case_statement} FROM {temp_table_name}"
        cte_start_part = f"WITH TEMP_TABLE_{i} AS ( " if i == start else f", TEMP_TABLE_{i} AS ( "
        cte_end_part = f"SELECT * FROM TEMP_TABLE_{i}"

        final_query = final_query + f"{cte_start_part} {sql_statement} ) "
        if i == end:
            final_query = final_query + cte_end_part
            final_query = final_query.format_map(param_dict)
            print("********************** FINAL QUERY ***************************")
            print(sqlparse.format(final_query, reindent=True, keyword_case='upper'))

        df.createOrReplaceTempView("TEMP_TABLE")
        enriched_df = df.sparkSession.sql(final_query)
        return enriched_df
