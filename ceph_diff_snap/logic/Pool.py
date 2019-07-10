# -*- coding:utf-8 -*-
import json
import subprocess
from StringIO import StringIO

import rbd

from Common.CephUtils import CephUtils
from Common.Logger import Logger
from Common.ThreadRunner import RbdDiffThreadRunner
from logic.Rbd import Rbd


class Pool(object):

    def __init__(self, ceph_conf, pool_name):
        self.ceph_conf = ceph_conf
        self.pool_name = pool_name
        #self.conn = CephUtils.connect_cluster(ceph_conf, pool_name) #ceph api有问题，只能用with的形式

    def get_rbd_list(self):
        with CephUtils.connect_cluster(self.ceph_conf, self.pool_name) as cluster:
            with cluster.open_ioctx(self.pool_name) as ioctx:
                rbd_inst = rbd.RBD()
                image_list = rbd_inst.list(ioctx)
        return image_list

    def remove_rbd(self, rbd_name):
        Rbd.remove_rbd_util(self.ceph_conf, self.pool_name, rbd_name)

    def create_rbd(self, rbd_name, size_byte):
        Rbd.create_rbd_util(self.ceph_conf, self.pool_name, rbd_name, size_byte)

    def get_rbd_size_byte(self, rbd_name):
        Rbd.get_rbd_size_byte_util(self.ceph_conf, self.pool_name, rbd_name)

    @staticmethod
    def diff(master_cluster, ceph_conf_master, ceph_conf_slave, pool_name):
        masterPool = Pool(ceph_conf_master, pool_name)
        slavePool = Pool(ceph_conf_slave, pool_name)
        masterRbds = masterPool.get_rbd_list()
        slavaRbds = slavePool.get_rbd_list()
        Logger().info("master pool %s rbd : %s" % (pool_name, masterRbds))
        Logger().info("slave pool %s rbd : %s" % (pool_name, slavaRbds))
        for rbdName in slavaRbds:
            if rbdName not in masterRbds:
                #删除slave里多余的rbd
                Logger().info("slave pool %s remove rbd %s" % (pool_name, rbdName))
                slavePool.remove_rbd(rbdName)
        for rbdName in masterRbds:
            if rbdName not in slavaRbds:
                #在slave添加没有的rbd
                Logger().info("slave pool %s create rbd %s" % (pool_name, rbdName))
                slavePool.create_rbd(rbdName, 1)
        ths = []
        for rbdName in masterRbds:
            master_cluster.semaphore.acquire()
            th = RbdDiffThreadRunner(master_cluster, ceph_conf_master, ceph_conf_slave, {"pool_name": pool_name, "rbd_name": rbdName},
                                     {"pool_name": pool_name, "rbd_name": rbdName})
            th.start()
            ths.append(th)
        return ths

    @staticmethod
    def pool_status(poolName, cephConf):
        props = {}
        output = subprocess.Popen(" ".join(['ceph --id admin', '-c', cephConf['conffile'],
                                            ' --keyring', cephConf['keyring'], 'osd dump --format=json']),
                                  shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0]
        pools = json.load(StringIO(output))
        pools = pools["pools"]
        for pool in pools:
            strName = pool["pool_name"]
            if strName == poolName:
                props["app_meta"] = pool["application_metadata"]
                props["pg"] = pool['pg_num']
                props["pgp"] = pool['pg_placement_num']
                break
        return props

    @staticmethod
    def set_pool_property(poolName, cephConf, prop):
        output = subprocess.Popen(" ".join(['ceph --id admin', '-c', cephConf['conffile'],
                                            ' --keyring', cephConf['keyring'], 'osd pool application enable',
                                            poolName, prop]),
                                  shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        lines = output.stdout.readlines()
        if lines[0].startswith("Error"):
            returnCode = -1
        else:
            returnCode = 0
        return returnCode, lines

    @staticmethod
    def create_pool_util(poolName, cephConf, pg, pgp):
        output = subprocess.Popen(" ".join(['ceph --id admin', '-c', cephConf['conffile'],
                                            ' --keyring', cephConf['keyring'],
                                            'osd pool create', poolName, str(pg), str(pgp)]),
                                  shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        lines = output.stdout.readlines()
        if lines[0].startswith("Error"):
            returnCode = -1
            raise Exception((returnCode, lines))
        else:
            returnCode = 0
        return returnCode, lines