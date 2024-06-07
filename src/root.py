import os

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
if ".zip" in ROOT_DIR:
    ROOT_DIR = "./"

SPARK_CONF_PATH = os.path.join(ROOT_DIR, "resources", "conf", "spark-default-configs.ini")
ENV_CONF_PATH = os.path.join(ROOT_DIR, "resources", "conf", "env-config.json")
RUNTIME_CONF_PATH = os.path.join(ROOT_DIR, "resources", "conf", "runtime-config.json")
TEST_DATA_DIR = os.path.join(ROOT_DIR, "data", "test_data")
OUTPUT_DATA_DIR = os.path.join(ROOT_DIR, "data", "output_data")
LOCAL_DATA_DIR = os.path.join(ROOT_DIR, "data", "sample_data")
