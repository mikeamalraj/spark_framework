from __future__ import annotations
import configparser
import os
import json


def to_dict(config: configparser.ConfigParser) -> dict[str, dict[str, str]]:
    """
    function converts a ConfigParser structure into a nested dict
    Each section name is a first level key in the dict, and the key values of the section becomes the dict in the second level
    {
        'section_name': {
            'key': 'value'
        }
    }
    Note: Any values defined in [DEFAULT] section are propagated to every other sections.
    :param config: the ConfigParser with the file already loaded
    :return: a nested dict
    """
    return {section_name: dict(config[section_name]) for section_name in config.sections()}


def read_config_file(filepath: str):
    if isinstance(filepath, str) and os.path.exists(filepath):
        config = configparser.ConfigParser()
        with open(filepath) as file:
            config.read_file(file)
            return to_dict(config)
    else:
        print(f"Exception occurred or path {filepath} not found!")


def read_json_file(filepath: str):
    if isinstance(filepath, str) and os.path.exists(filepath):
        with open(filepath) as f:
            env_config = json.loads(f.read())
            return env_config
    else:
        print(f"Exception occurred or path {filepath} not found!")


if __name__ == "__main__":
    from src.root import SPARK_CONF_PATH
    data = read_config_file(SPARK_CONF_PATH)
    print(data)