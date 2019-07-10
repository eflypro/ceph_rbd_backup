#!/usr/bin/env python
# -*- coding: utf-8 -*-
# made by likunxiang


from optparse import OptionParser

from Common.GlobalVar import GlobalVar
from Common.Logger import Logger
from Common.CephUtils import CephUtils
import sys

from logic.Cluster import Cluster

reload(sys)
sys.setdefaultencoding('utf-8')


class ArgsHandler:
    def __init__(self):
        pass

    @staticmethod
    def do_test_snap_diff(max_thread_num):
        GlobalVar.isTest = True
        ArgsHandler.do_snap_diff(max_thread_num)

    @staticmethod
    def do_snap_diff(max_thread_num):
        ceph_conf_master = CephUtils.get_ceph_conf("master")
        ceph_conf_slave = CephUtils.get_ceph_conf("slave")
        Cluster.diff(ceph_conf_master, ceph_conf_slave, max_thread_num)


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-s", "--snap-diff", action="store", dest="doSnapDiff",
                      help="snap diff from master with special thread num")
    parser.add_option("-t", "--test-snap-diff", action="store", dest="doTestSnapDiff",
                      help="test snap diff from master with special thread num")
    (option, args) = parser.parse_args()

    arg2Haneler = {
        "doSnapDiff": ArgsHandler.do_snap_diff,
        "doTestSnapDiff": ArgsHandler.do_test_snap_diff,
    }

    done = False
    for arg, handler in arg2Haneler.items():
        if hasattr(option, arg):
            argValue = getattr(option, arg)
            if argValue:
                done = True
                handler(argValue)
                break
    if not done:
        Logger().warning("do nothing")

    sys.exit(0)
