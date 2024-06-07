from __future__ import annotations
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.catalog import Column
import re
import os

from src.constants.lib_constants import *
from src.root import LOCAL_DATA_DIR, OUTPUT_DATA_DIR
from src.utils.helpers import *
from src.services.datasource_service import *
from src.utils.talk_to_db import TalkToDb
from src.utils.encryption import decrypt_message


def read_df_from_local(session: SparkSession, table_name: str, format="csv") -> DataFrame:
    df = session.createDataFrame(data=[], schema=StructType([]))
    options_conf = {}
    file_path = os.path.join(LOCAL_DATA_DIR, f"{table_name.lower()}")
    if format.upper() == "CSV":
        options_conf = {
            "header": True,
            "ignoreLeadingWhiteSpace": False,
            "ignoreTrailingWhiteSpace": False,
            "delimiter": ",",
            "timestampFormat": "yyyy/MM/dd HH:mm:ss ZZ",
            "charset": "UTF8",
            "inferSchema": True
        }
        file_path = file_path + ".csv"
    elif format.upper() == "PARQUET":
        file_path = file_path + ".parquet"

    print(f"local file path: {file_path}")

    if isinstance(file_path, str) and os.path.exists(file_path):
        df = session.read.format(format).options(**options_conf).load(file_path)
    else:
        print(f"Local file - {file_path} not found!")
    return df


def get_connection_props(spark, env):
    try:
        return {
            "driver": spark.conf.get(f"spark.{env}.jdbc.driver"),
            "user": spark.conf.get(f"spark.{env}.db.username"),
            "password": decrypt_message(os.getenv("SECRETKEY"), spark.conf.get(f"spark.{env}.db.password")),
            "url": spark.conf.get(f"spark.{env}.jdbc.url"),
            "numPartitions": "5",
            "fetchsize": "10000"
        }

    except Exception as e:
        raise Exception("Error occurred! Check spark properties!")


def read_df_from_db_query(spark: SparkSession, query: str, env: str):
    props = get_connection_props(spark, env.lower())
    option_dict = {
        "url": props['url'],
        "driver": props['driver'],
        "user": props["user"],
        "password": props["password"],
        "query": query,
        "numPartitions": props["numPartitions"],
        "fetchsize": props["fetchsize"]
    }

    df = spark.read.format("jdbc").options(**option_dict).load()
    return df


def read_df_from_db(spark: SparkSession, db_name: str, table_name: str, env: str):
    props = get_connection_props(spark, env.lower())
    url = props['url']
    df = spark.read.jdbc(url=url, table=f"{db_name}.{table_name}", properties=props)
    return df


def read_df_from_db_source(session: SparkSession, data_config: DataConfig, param_dict: dict) -> DataFrame:
    query: str = data_config.query
    if data_config.is_query == "Y" and (query != "" and query is not None):
        query = query.format_map(param_dict)
        print(f"Reading dataset from db using query: {query}")
        df = read_df_from_db_query(session, query, data_config.parameters.environment)
    else:
        df = read_df_from_db(session, data_config.database_name, data_config.table_name,
                             data_config.parameters.environment)

    return df.repartition(int(session.conf.get("spark.sql.shuffle.partitions")))


def read_df_from_hadoop(session: SparkSession, data_config: DataConfig, param_dict) -> DataFrame:
    df = session.createDataFrame([], StringType([]))
    if isinstance(data_config, SourceHiveConfig):
        query = data_config.query
        if data_config.is_query == "Y" and (query != "" and query is not None):
            query = query.format_map(param_dict)
            df = session.sql(query)
        else:
            df = session.read.table(data_config.database_name + "." + data_config.table_name)
    elif isinstance(data_config, SourceHadoopConfig):
        path = data_config.root_path
        print(f"Reading data from HDFS path : {path}")

        if data_config.format == PARQUET:
            df = session.read.parquet(path)
        elif data_config.format == CSV:
            df = session.read.option("header", True).option("delimiter", data_config.delimiter) \
                .option("ignoreLeadingWhiteSpace", False) \
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ") \
                .csv(path)
        else:
            df = session.read.format(data_config.format).load(path)
    return df


def read_df_from_source(session: SparkSession, data_config: DataConfig, env: str, skip_local_read: bool = False,
                        param_dict: dict = {}) -> DataFrame:
    df = session.createDataFrame(data=[], schema=StructType([]))
    if skip_local_read is False and (env.upper() == "LOCAL" or env.upper() == "TEST"):
        if isinstance(data_config, SourceHadoopConfig):
            df = read_df_from_local(session, data_config.file_name)
        else:
            df = read_df_from_local(session, data_config.table_name)
    elif isinstance(data_config, MariaDBConfig):
        df = read_df_from_db_source(session, data_config, param_dict)
    elif isinstance(data_config, SourceHiveConfig) or isinstance(data_config, SourceHadoopConfig):
        df = read_df_from_hadoop(session, data_config, param_dict)
    else:
        raise Exception("Invalid datasource!")

    if isinstance(data_config, MariaDBConfig) or isinstance(data_config, SourceHiveConfig) or isinstance(data_config,
                                                                                                         SourceHadoopConfig):
        if data_config.partition_filter is not None and data_config.partition_filter != "":
            partition_filter = data_config.partition_filter.format_map(param_dict)
            df = df.filter(partition_filter)

    if data_config.filter_max_version_key is not None and data_config.filter_max_version_key.upper() == "Y":
        max_version_key = df.select("version_key").selectExpr("max(version_key)").collect()[0][0]
        if max_version_key is not None:
            if isinstance(data_config, MariaDBConfig) or isinstance(data_config, SourceHiveConfig):
                dataset_name = data_config.table_name
            elif isinstance(data_config, SourceHadoopConfig):
                dataset_name = data_config.root_path
            print(f"Latest version_key of {dataset_name} is : {max_version_key}")
            df = df.filter(f"version_key='{max_version_key}'")

    if data_config.filter_condition is not None and data_config.filter_condition != "":
        df = df.filter(data_config.filter_condition.format_map(param_dict))

    if data_config.select_cols is not None and len(data_config.select_cols) > 0:
        df = df.select(data_config.select_cols)

    return df


def save_data_to_local(df: DataFrame, dataset_name: str, format="csv", mode="overwrite"):
    file_path = os.path.join(OUTPUT_DATA_DIR, f"{dataset_name}")
    print(f"Local file path: {file_path}")
    option_dict = {
        "header": True,
        "delimiter": ","
    }
    spark = df.sparkSession
    spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs", False)
    df.coalesce(1).write.format(format).options(**option_dict).mode(mode).save(file_path)


def save_data_to_db(df: DataFrame, data_config: DataConfig, env: str, db_obj: TalkToDb = None):
    props = get_connection_props(df.sparkSession, env.lower())

    options_dict = {
        "url": props['url'],
        "driver": props['driver'],
        "user": props['user'],
        "password": props['password'],
        "dbtable": f"{data_config.database_name}.{data_config.table_name}",
        "numPartitions": 5,
        "batchsize": 10000
    }

    columns = data_config.target_column

    if len(columns) == 0:
        query = f"SELECT * FROM {data_config.database_name}.{data_config.table_name} WHERE 1=0"
        empty_df = read_df_from_db_query(df.sparkSession, query, env)
        db_columns = get_lower_list(empty_df.columns)
        df_columns = get_lower_list(df.columns)
        select_columns = [col_name for col_name in db_columns if col_name in df_columns]
        missing_in_df = [col_name for col_name in db_columns if col_name not in df_columns]
        print(f"Columns that are not present in result df: {missing_in_df}")
        missing_in_db = [col_name for col_name in df_columns if col_name not in db_columns]
        print(f"Columns that are not present in database: {missing_in_db}")

    # Explicitly overwriting here because Spark will drop and recreate the table when write mode is overwrite
    if data_config.write_mode.lower() == OVERWRITE and data_config.overwrite_condition is not None and db_obj is not None:
        query = f"DELETE FROM {data_config.database_name}.{data_config.table_name} where {data_config.overwrite_condition}"
        print(f"Query: {query}")
        db_obj.execute_query(query)

    df = df.select(select_columns).repartition(options_dict['numPartitions'] * 2)
    df.write.format("jdbc").mode("append").options(**options_dict).save()


def save_data_to_hive(df: DataFrame, data_config: DataConfig):
    data_path = f""
    {data_config.root_path}
    col_metadata: list[Column] = df.sparkSession.catalog.listColumns(data_config.table_name, data_config.database_name)
    df = cast_df_datatype(df, col_metadata)
    selected_cols = [obj.name for obj in col_metadata if obj.name in df.columns]
    df = df.select(selected_cols)
    df.write.format(data_config.format) \
        .mode(data_config.write_mode) \
        .option("path", data_path) \
        .insertInto(f"{data_config.database_name}.{data_config.table_name}")


def save_data_to_hadoop(df: DataFrame, data_config: DataConfig):
    data_path = f"{data_config.root_path}"
    columns = data_config.target_column + data_config.partition_by_cols

    if len(columns) == 0:
        columns = df.columns
    df = df.select(columns)
    if len(data_config.partition_by_cols) > 0:
        df.write.partitionBy(data_config.partition_by_cols) \
            .format(data_config.format) \
            .mode(data_config.write_mode) \
            .save(data_path)
    else:
        df.write.format(data_config.format) \
            .mode(data_config.write_mode).save(data_path)


def save_dataframe(df: DataFrame, data_config: DataConfig, env: str, skip_local_write: bool = False,
                   db_obj: TalkToDb = None):
    try:
        if skip_local_write is False and (env.upper() == "LOCAL" or env.upper() == "TEST"):
            save_data_to_local(df, data_config.table_name)
        elif isinstance(data_config, TargetMariaDBConfig):
            save_data_to_db(df, data_config, env, db_obj)
        elif isinstance(data_config, TargetHiveConfig):
            save_data_to_hive(df, data_config)
        elif isinstance(data_config, TargetHadoopConfig):
            save_data_to_hadoop(df, data_config)
        else:
            raise Exception("Invalid target!")
    except:
        raise f"Error occurred while saving data!"
    else:
        print("Dataframe saved successfully!")


def cast_df_datatype(df: DataFrame, col_metadata: list[Column]):
    df_columns = get_lower_list(df.columns)
    missing_in_df = [obj.name for obj in col_metadata if obj.name.lower() not in df_columns]
    print(f"Columns that are not present in result df: {missing_in_df}")
    missing_in_db = [col_name for col_name in df_columns if col_name not in [obj.name.lower() for obj in col_metadata]]
    print(f"Columns that are not present in database: {missing_in_db}")
    cast_operations_list: list[Column] = list()

    for obj in col_metadata:
        if str.lower(obj.dataType) == 'int':
            data_type = IntegerType()
        elif str.lower(obj.dataType) == 'boolean':
            data_type = BooleanType()
        elif str.lower(obj.dataType) == 'timestamp':
            data_type = TimestampType()
        elif str.lower(obj.dataType) == 'double':
            data_type = DoubleType()
        elif str.lower(obj.dataType) == 'bigint':
            data_type = DoubleType()
        elif str.lower(obj.dataType).startswith('decimal'):
            decimal_val = re.sub("[^\\d,.]", "", obj.dataType).split(",")
            data_type = DecimalType(int(decimal_val[0]), int(decimal_val[1]))
        else:
            data_type = StringType()

        if obj.name.lower() in df_columns:
            cast_operations_list.append(col(obj.name).cast(data_type).alias(obj.name))
        else:
            cast_operations_list.append(lit(None).cast(data_type).alias(obj.name))

    return df.select(cast_operations_list)
