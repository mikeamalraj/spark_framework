from __future__ import annotations
from pyspark.sql import *
from pyspark.sql import functions as F
from src.utils.enrichment import Enrichment


def get_enriched_df(df: DataFrame, with_col_list: list[tuple[str, Column]] = list(),
                    rename_col_list: list[tuple[str, str]] = list(),
                    derived_col_list: list[tuple[str, str]] = list(),
                    default_col_list: list[tuple[str, str]] = list(),
                    with_expr_list: list[tuple[str, str]] = list(),
                    case_cond_list: list[tuple[str, str]] = list(),
                    drop_columns: list[str] = list(), profile: str = None) -> DataFrame:
    en = Enrichment(df.columns, profile)
    en.set_with_columns(with_col_list)
    en.set_rename_columns(rename_col_list)
    en.set_derived_columns(derived_col_list)
    en.set_default_columns(default_col_list)
    en.set_with_expr_columns(with_expr_list)
    en.set_case_cond_columns(case_cond_list)
    en.set_drop_columns(drop_columns)
    return en.get_result_df(df)


def get_with_col_df(df: DataFrame, with_col_list: list[tuple[str, Column]] = list()) -> DataFrame:
    en = Enrichment(df.columns)
    en.set_with_columns(with_col_list)
    return en.get_result_df(df)


def get_rename_col_df(df: DataFrame, rename_col_list: list[tuple[str, str]] = list()) -> DataFrame:
    en = Enrichment(df.columns)
    en.set_rename_columns(rename_col_list)
    return en.get_result_df(df)


def get_derived_col_df(df: DataFrame, derived_col_list: list[tuple[str, str]] = list()) -> DataFrame:
    en = Enrichment(df.columns)
    en.set_derived_columns(derived_col_list)
    return en.get_result_df(df)


def get_default_col_df(df: DataFrame, default_col_list: list[tuple[str, str]] = list()) -> DataFrame:
    en = Enrichment(df.columns)
    en.set_default_columns(default_col_list)
    return en.get_result_df(df)


def get_with_expr_col_df(df: DataFrame, with_expr_list: list[tuple[str, str]] = list()) -> DataFrame:
    en = Enrichment(df.columns)
    en.set_with_expr_columns(with_expr_list)
    return en.get_result_df(df)


def get_case_cond_col_df(df: DataFrame, case_cond_list: list[tuple[str, str]] = list()) -> DataFrame:
    en = Enrichment(df.columns)
    en.set_case_cond_columns(case_cond_list)
    return en.get_result_df(df)


def get_drop_col_df(df: DataFrame, drop_columns: list[str] = list()) -> DataFrame:
    en = Enrichment(df.columns)
    en.set_drop_columns(drop_columns)
    return en.get_result_df(df)


def replace_empty_columns(df: DataFrame):
    cols = df.columns
    replaced_cols = [F.when(F.col(col_name) == "", F.lit(None)).otherwise(F.col(col_name)).alias(col_name) for col_name
                     in cols]
    return df.select(replaced_cols)


def drop_cols_with_prefix(df: DataFrame, prefix: list):
    drop_cols = [col_name for col_name in df.columns for drop_prefix in prefix if col_name.startswith(drop_prefix)]
    return get_drop_col_df(df, drop_cols)


def rename_cols_with_prefix(df: DataFrame, prefix: str):
    rename_col_list = [(col_name, f"`{prefix}_{col_name}`") for col_name in df.columns]
    return get_rename_col_df(df, rename_col_list)


def get_pivot_df(df: DataFrame, group_by_cols: list, pivot_col: str, value_col: str) -> DataFrame:
    return df.groupby(*group_by_cols).pivot(pivot_col).max(value_col)


def cast_decimal_to_double(df: DataFrame) -> DataFrame:
    col_list = []
    for col_name, data_type in df.dtypes:
        if str.lower(data_type).startswith('decimal'):
            col_list.append(f"cast(`{col_name}` as double) as `{col_name}`")
        else:
            col_list.append(f"`{col_name}`")
    return df.selectExpr(*col_list)


if __name__ == "__main__":
    pass
