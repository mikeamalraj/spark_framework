import argparse
import sys
import platform


class BaseParam:
    def __init__(self):
        self.is_debug = None
        self.skip_local = None
        self.is_debug = None
        self.runtime_conf = None
        self.log_level = None
        self.job_name = None
        self.stress_model = None
        self.environment = None
        self.parent_parser = self.get_parent_parser()

    def init_args(self, args_dicts):
        self.environment = args_dicts['environment'].upper().strip()
        self.stress_model = args_dicts['stress_model']
        self.job_name = args_dicts['job_name']
        self.log_level = args_dicts['log_level']
        self.runtime_conf = args_dicts['runtime_conf']
        self.is_debug = args_dicts['debug']
        self.skip_local = args_dicts['skip_local']

    def get_parent_parser(self):
        parent_parser = argparse.ArgumentParser(add_help=False)
        parent_parser.add_argument('environment', type=str, help="Description of environment")
        parent_parser.add_argument('job_name', type=str, help="Name of the job")
        parent_parser.add_argument('--stress_model', type=str, default='Power', help="Name of stress test model")
        parent_parser.add_argument('--log_level', type=str, help="Log level of spark process")
        parent_parser.add_argument('--runtime_conf', type=str, help="Path to runtime_conf file")
        parent_parser.add_argument('--debug', action='store_true', help="To execute in debug mode")
        parent_parser.add_argument('--skip_local', action='store_true', help="Skip read/write from/to local machine")
        return parent_parser


class Param(BaseParam):
    def __init__(self, params, script_name: str):
        super().__init__()
        args_dict = self.parse_arguments(params, script_name, self.parent_parser)
        self.init_args(args_dict)
        self.run_id = args_dict['run_id']
        self.scenario_set_id = args_dict['scenario_set_id']
        self.borrower_id = args_dict['borrower_id']

    def parse_arguments(self, args, script_name: str, parent_parser):
        parser = argparse.ArgumentParser(prog=script_name, description=f"ARGUMENTS LIST : {script_name.upper()}",
                                         parents=[parent_parser])
        parser.add_argument("run_id", type=str, help="Run id of the current execution")
        parser.add_argument("scenario_set_id", type=str, help="scenario_set_id of the current execution")
        parser.add_argument("borrower_id", type=str, help="borrower_id of the current execution")
        return vars(parser.parse_args(args))

    def __str__(self):
        display_str = f"Following are the parameters passed to job => environment: {self.environment}, stress_model: {self.stress_model} "
        return display_str


if __name__ == "__main__":
    is_local = platform.system().startswith("Windows")
    show_help = True

    if show_help:
        cmd_args = "-h".split()
    elif is_local:
        cmd_args = "local test_application 22 33 55 --debug --skip_local".split()
    else:
        cmd_args = sys.argv[1:]

    param = Param(cmd_args, "test")
    print(param)
