# -*- coding:utf-8 -*-
import threading

from Common.Logger import Logger
from logic.Rbd import Rbd


class RbdDiffThreadRunner (threading.Thread):
    def __init__(self, master_cluster, ceph_conf_master, ceph_conf_slave, master_rbd_pool_path, slave_rbd_pool_path):
        threading.Thread.__init__(self)
        self.master_cluster = master_cluster
        self.ceph_conf_master = ceph_conf_master
        self.ceph_conf_slave = ceph_conf_slave
        self.master_rbd_pool_path = master_rbd_pool_path
        self.slave_rbd_pool_path = slave_rbd_pool_path
        self.threadID = "tid" + master_rbd_pool_path['pool_name'] + "_" + master_rbd_pool_path['rbd_name']

    def run(self):
        try:
            Rbd.diff(self.ceph_conf_master, self.ceph_conf_slave, self.master_rbd_pool_path, self.slave_rbd_pool_path)
        except Exception, e:
            Logger().info(e)
        finally:
            self.master_cluster.semaphore.release()



