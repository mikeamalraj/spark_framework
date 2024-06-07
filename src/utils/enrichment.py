from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class Enrichment:

    def __init__(self, source_cols, profile=None):
        self.temp_col_list = None
        self.source_columns: list[str] = [x.lower() for x in source_cols]
        self.target_columns: list[F.Column] = list()
        self.profile = profile

    def set_with_columns(self, col_list: list[tuple[str, F.Column]]):
        self.update_source_columns(col_list)
        self.update_target_columns([item[1].alias(item[0].lower()) for item in col_list])

    def set_rename_columns(self, col_list: list[tuple[str, str]]):
        self.update_source_columns(col_list)
        self.update_target_columns([F.col(item[0]).alias(item[1].lower()) for item in col_list])

    def set_derived_columns(self, col_list: list[tuple[str, str]]):
        self.update_source_columns(col_list)
        self.update_target_columns([F.col(item[1]).alias(item[0].lower()) for item in col_list])

    def set_default_columns(self, col_list: list[tuple[str, str]]):
        self.update_source_columns(col_list)
        self.update_target_columns([F.lit(item[1]).alias(item[0].lower()) for item in col_list])

    def set_with_expr_columns(self, col_list: list[tuple[str, str]]):
        self.update_source_columns(col_list)
        self.update_target_columns([F.expr(item[1]).alias(item[0].lower()) for item in col_list])

    def set_case_cond_columns(self, col_list: list[tuple[str, str]]):
        self.update_source_columns(col_list)
        self.update_target_columns([F.expr(item[1]).alias(item[0].lower()) for item in col_list])

    def set_drop_columns(self, col_list: list[str]):
        self.update_source_columns(col_list, False)

    def update_source_columns(self, col_list, list_of_tuples: bool = True):
        if list_of_tuples:
            self.temp_col_list = [item[0].lower() for item in col_list]
        else:
            self.temp_col_list = [item.lower() for item in col_list]

        if col_list:
            self.source_columns = [col_name for col_name in self.source_columns if col_name not in self.temp_col_list]

    def update_target_columns(self, col_list):
        if col_list:
            self.target_columns += col_list

    def get_target_columns(self):
        return [F.col(col_name) for col_name in self.source_columns] + self.target_columns

    def get_duplicate_list(self, col_list):
        unique_list = []
        duplicate_list = []
        for i in col_list:
            if i not in unique_list:
                unique_list.append(i)
            elif i not in duplicate_list:
                duplicate_list.append(i)
        return duplicate_list

    def get_result_df(self, df: DataFrame):
        result_columns = self.get_target_columns()
        result_df = df.select([x for x in result_columns])
        duplicates = self.get_duplicate_list(result_df.columns)
        if duplicates:
            print(f"One or more duplicate columns {duplicates} found in profile: {self.profile}")
        return result_df
