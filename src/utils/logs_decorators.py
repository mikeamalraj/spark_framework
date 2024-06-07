from .log import LogUtils


def log_df_reader(func):
    def inner1(*args, **kwargs):
        logger = LogUtils(application_name=func.__name__,
                          format_string="%(levelname)s --%(name)s %(message)s").get_logger()
        func_args_str = args[1]
        logger.info(f"execution started -> {func_args_str}")

        # getting the returned value
        returned_value = func(*args, *kwargs)

        logger.info(f"execution finished -> {func_args_str}")

        return returned_value

    return inner1


def simple_logger(func):
    logger = LogUtils(application_name=func.__name__,
                      format_string="-- %(levelname)s -- %(name)s %(message)s").get_logger()

    def inner1(*args, **kwargs):
        func_args_str = args[1]
        logger.info(f"Execution started ->  {func_args_str}")

        # getting returned value
        func(*args, **kwargs)
        logger.info(f"Execution finished -> {func_args_str}")

    return inner1
