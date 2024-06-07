from src.core.params import BaseParam
from src.constants.lib_constants import *


class DataConfig:
    pass


class HiveConfig(DataConfig):
    def __init__(self, database_name, table_name):
        self.database_name = database_name
        self.table_name = table_name

    def __str__(self):
        return f"HiveConfig: database_name: {self.database_name}, table_name: {self.table_name}"


class SourceHiveConfig(HiveConfig):
    def __init__(self, database_name, table_name, partition_filter: str, filter_condition: str,
                 filter_max_version_key: str, select_cols: str, is_query, query: str, parameters: BaseParam):
        super().__init__(database_name, table_name)
        self.partition_filter = partition_filter
        self.filter_condition = filter_condition
        self.filter_max_version_key = filter_max_version_key
        self.select_cols = select_cols
        self.is_query = is_query
        self.query = query
        self.parameters = parameters

    def __str__(self):
        return f"SourceHiveConfig: database_name: {self.database_name}, table_name: {self.table_name}, " \
               f"partition_filter: {self.partition_filter}, filter_condition: {self.filter_condition}"


class TargetHiveConfig(HiveConfig):

    def __init__(self, root_path, database_name, table_name, partition_by_cols, target_column, write_mode=OVERWRITE,
                format="parquet"):
        super().__init__(database_name, table_name)
        self.root_path = root_path
        self.partition_by_cols = partition_by_cols
        self.target_column = target_column
        self.write_mode = write_mode
        self.format = format
        self.overwrite_condition = None

    def __str__(self):
        return f"TargetHiveConfig: database_name: {self.database_name}, table_name: {self.table_name}, " \
               f"partition_by_cols: {self.partition_by_cols}"


class MariaDBConfig(DataConfig):
    def __init__(self, database_name, table_name, partition_filter: str, filter_condition: str,
                 filter_max_version_key: str, select_cols: list, is_query, query: str, param: BaseParam):
        self.database_name = database_name
        self.table_name = table_name
        self.partition_filter = partition_filter
        self.filter_condition = filter_condition
        self.filter_max_version_key = filter_max_version_key
        self.select_cols = select_cols
        self.is_query = is_query
        self.query = query
        self.parameters = param

    def __str__(self):
        return f"MariaDBConfig: database_name: {self.database_name}, table_name: {self.table_name}, " \
               f"partition_filter: {self.partition_filter}"


class TargetMariaDBConfig(DataConfig):
    def __init__(self, database_name, table_name, partition_by_cols, target_column, write_mode,
                 overwrite_condition: None):
        self.database_name = database_name
        self.table_name = table_name
        self.partition_by_cols = partition_by_cols
        self.target_column = target_column
        self.write_mode = write_mode
        self.overwrite_condition = overwrite_condition

    def __str__(self):
        return f"TargetMariaDBConfig: database_name: {self.database_name}, table_name: {self.table_name}"


class HadoopConfig(DataConfig):
    def __init__(self, root_path):
        self.root_path = root_path

    def __str__(self):
        return f"HadoopConfig: root_path: {self.root_path}"


class SourceHadoopConfig(HadoopConfig):
    def __init__(self, root_path, partition_filter: str, filter_condition: str, filter_max_version_key: str,
                 select_cols: list, param: BaseParam, format="parquet", delimiter=None):
        super().__init__(root_path)
        self.partition_filter = partition_filter
        self.filter_condition = filter_condition
        self.filter_max_version_key = filter_max_version_key
        self.select_cols = select_cols
        self.parameters = param
        self.format = format
        self.delimiter = delimiter

    def __str__(self):
        return f"SourceHadoopConfig: root_path: {self.root_path}, partition_filter: {self.partition_filter}, " \
               f"filter_condition: {self.filter_condition}"


class TargetHadoopConfig(HadoopConfig):
    def __init__(self, root_path, file_name, partition_by_cols, target_column, write_mode, format="parquet",
                 delimiter=None):
        super().__init__(root_path)
        self.file_name = file_name
        self.partition_by_cols = partition_by_cols
        self.target_colum = target_column
        self.write_mode = write_mode
        self.format = format
        self.delimiter = delimiter
        self.overwrite_condition = None

    def __str__(self):
        return f"TargetHadoopConfig: root_path: {self.root_path}, file_name: {self.file_name}, format: {self.format}, " \
               f"write_mode: {self.write_mode}"
