# -*- coding:utf-8 -*-
import time


class GlobalVar:
    """
    全局变量
    """
    now_ts = time.time() * 1000

    snap_name_prefix = 'auto_'

    isTest = False

    log_file_path = "/var/log/CephAPI/"

    host = "localhost"
    user = "rjiaas"
    passwd = "rjkj@rjkj"
    dbname = "ceph_diff"

    def __init__(self):
        pass

class LogLevel:
    """
    日志等级
    """

    def __init__(self):
        pass
    # def

    INFO = "INFO"  # 普通日志
    ERROR = "ERROR"  # 错误日志
