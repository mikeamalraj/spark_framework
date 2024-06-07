from src.constants.lib_constants import UPPER, LOWER


class Default(dict):

    def __missing__(self, key):
        return key.join("{}")


def get_lower_list(str_list):
    return [item.lower().strip() for item in str_list]


def get_upper_list(str_list):
    return [item.upper().strip() for item in str_list]


def get_list_from_string(string: str, cast_type: str = UPPER):
    res_list = [item.strip() for item in string.split(",")] if (string != "" and string is not None) else []
    if cast_type == UPPER:
        return get_upper_list(res_list)
    elif cast_type == LOWER:
        return get_lower_list(res_list)
    else:
        return res_list


def get_upper_str(string: str):
    return string.upper().strip() if (string is not None and string != "") else None


def get_lower_str(string: str):
    return string.lower().strip() if (string is not None and string != "") else None
