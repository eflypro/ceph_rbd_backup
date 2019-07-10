# -*- coding:utf-8 -*-
import threading

from Common.CephUtils import CephUtils
from Common.GlobalVar import GlobalVar
from Common.Logger import Logger
from Common.Util import Util
from logic.Pool import Pool


class Cluster(object):

    def __init__(self, ceph_conf, is_controller=False, max_thread_num=4):
        self.ceph_conf = ceph_conf
        self.conn = CephUtils.connect_cluster(ceph_conf)
        self.pools_properties = {}
        if is_controller:
            max_thread_num = max_thread_num if max_thread_num > 0 else 4
            Logger().info("%d threads will be started for doing diff snap" % (max_thread_num, ))
            self.semaphore = threading.Semaphore(max_thread_num) #最多同时n个线程diff

        pools = self.conn.list_pools()
        for poolName in pools:
            if GlobalVar.isTest:
                if not poolName.startswith("test"):
                    continue
            else:
                if poolName.startswith(".") or poolName.startswith("default"):
                    continue
            self.pools_properties[poolName] = Pool.pool_status(poolName, ceph_conf)
        Logger().info(self.ceph_conf['name'] + " pool info: " + repr(self.pools_properties))

    def delete_pool(self, pool_name):
        try:
            self.conn.delete_pool(pool_name)
        except Exception, e:
            raise Exception("delete pool %s error:%s" % (pool_name, repr(e)))

    def create_pool(self, pool_name, pg, pgp):
        ret = 0
        try:
            #self.conn.create_pool(pool_name) 这个不知怎样设置pg pgp
            Pool.create_pool_util(pool_name, self.ceph_conf, pg, pgp)
            Pool.set_pool_property(pool_name, self.ceph_conf, 'rbd')
        except Exception, e:
            Logger().error("create pool %s error:%s" % (pool_name, repr(e)))
            ret = -1
        return ret

    def shutdown_conn(self):
        self.conn.shutdown()

    @staticmethod
    def diff(ceph_conf_master, ceph_conf_slave, max_thread_num):
        if not max_thread_num.isdigit():
            max_thread_num = 0
        master_cluster = Cluster(ceph_conf_master, True, int(max_thread_num))
        slave_cluster = Cluster(ceph_conf_slave)
        # 看看slave与master的pool有什么差异，以master为准
        for poolName, properties in slave_cluster.pools_properties.items():
            if poolName in master_cluster.pools_properties.keys():
                if "rbd" not in properties["app_meta"]:
                    set_prop_res = Pool.set_pool_property(poolName, slave_cluster.ceph_conf, "rbd")
                    Logger().info("set pool %s rbd property return : %s" % (poolName, repr(set_prop_res)))
                continue
            # 删除slave多余的pool
            Logger().info("slave cluster remove pool " + poolName)
            slave_cluster.delete_pool(poolName)

        for poolName, properties in master_cluster.pools_properties.items():
            if poolName in slave_cluster.pools_properties.keys():
                continue
                # slave添加新pool
            Logger().info("slave cluster create pool " + poolName)
            slave_cluster.create_pool(poolName, properties['pg'], properties['pgp'])

        master_cluster.shutdown_conn()
        slave_cluster.shutdown_conn()

        working_threads = []
        for poolName in master_cluster.pools_properties.keys():
            ths = Pool.diff(master_cluster, ceph_conf_master, ceph_conf_slave, poolName)
            working_threads.extend(ths)

        for th in working_threads:
            th.join()

        #清理/tmp目录
        Util.rmdirs("/tmp/" + str(GlobalVar.now_ts))

