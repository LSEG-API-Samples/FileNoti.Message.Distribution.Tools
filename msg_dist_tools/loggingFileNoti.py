import logging
import os, sys
from pathlib import Path

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

app_loggers = {}
error_loggers = {}


def setup_logger(name, log_file, level=logging.INFO, log_sys_type=sys.stdout):
    """To setup as many loggers as you want"""

    log_path = Path(log_file)
    log_dir = log_path.parent
    print("log_path {}, log_dir {}".format(log_path, log_dir))
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    if log_sys_type is not None:
        sout_handler = logging.StreamHandler(log_sys_type)
        sout_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    if log_sys_type is not None:
        logger.addHandler(sout_handler)

    return logger


def get_app_logger(logger_name, log_file="log/app.log"):
    global app_loggers

    if app_loggers.get(logger_name):
        return app_loggers.get(logger_name)
    else:
        app_logger = setup_logger(logger_name, log_file)
        app_loggers[logger_name] = app_logger
        return app_logger


def get_error_logger(logger_name, log_file="log/error.log"):
    global error_loggers

    if error_loggers.get(logger_name):
        return error_loggers.get(logger_name)
    else:
        error_logger = setup_logger(logger_name, log_file, level=logging.ERROR, log_sys_type=None)
        error_loggers[logger_name] = error_logger
        return error_logger


#app_logger = setup_logger("app_info", "app.log")
#error_logger = setup_logger("app_error", "error.log", level=logging.ERROR, log_sys_type=sys.stderr)

# def convert_data_to_tabular(data_dict_list):
#     header = data_dict_list[0].keys()
#     for idx, x in enumerate(data_dict_list):
#         for y in x.keys():
#             print(x[y])
#             data_dict_list[idx][y] = wrapper.wrap(text=str(x[y]))
#     rows = [x.values() for x in data_dict_list]
#     return tabulate.tabulate(rows, header)

# logging.exception("Deliberate divide by zero traceback")

'''
>>> import logging
>>> try:
...     1/0
... except ZeroDivisionError:
...     logging.exception("Deliberate divide by zero traceback")
... 
ERROR:root:Deliberate divide by zero traceback
Traceback (most recent call last):
  File "<stdin>", line 2, in <module>
ZeroDivisionError: integer division or modulo by zero
'''

'''
>>> import logging
>>> logging.basicConfig(level=logging.DEBUG)
>>> logging.getLogger().info('This prints the stack', stack_info=True)
INFO:root:This prints the stack
Stack (most recent call last):
  File "<stdin>", line 1, in <module>
>>>
'''