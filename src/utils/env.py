import os
from src.root import RUNTIME_CONF_PATH
from src.utils.fileutil import read_json_file


def set_environment_var(filepath: str, env: str):
    if isinstance(filepath, str):
        config = read_json_file(filepath)
    else:
        config = read_json_file(RUNTIME_CONF_PATH)

    if not config:
        error_message = f"runtime-config path is : {config}"
        raise Exception(error_message)

    if env in config:
        for key, val in config[env].items():
            os.environ[key.upper()] = val
    else:
        print(f"{env} not found in run-time config: {filepath}")
