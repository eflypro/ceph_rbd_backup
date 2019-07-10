# -*- coding:utf-8 -*-
import subprocess
import time

import rbd

from Common.CephUtils import CephUtils
from Common.GlobalVar import GlobalVar
from Common.Logger import Logger
from Common.Mysql import MySql
from Common.Util import Util


class Rbd(object):
    def __init__(self, ceph_conf, pool_name, rbd_name):
        self.ceph_conf = ceph_conf
        self.pool_name = pool_name
        self.rbd_name = rbd_name

    def get_snap_list(self, prefix_string=None):
        snaps = Rbd.get_rbd_snap_list_util(self.ceph_conf, self.pool_name, self.rbd_name)
        if not prefix_string:
            return snaps

        snap_infos = []
        for snap_info in snaps:
            if snap_info['name'].startswith(prefix_string):
                snap_infos.append(snap_info)
        return snap_infos

    def create_snap(self, snap_name):
        return Rbd.create_rbd_snap_util(self.ceph_conf, self.pool_name, self.rbd_name, snap_name)

    def remove_snap(self, snap_name):
        return Rbd.remove_rbd_snap_util(self.ceph_conf, self.pool_name, self.rbd_name, snap_name)

    def clean_snaps(self, prefix_string=None):
        snaps = self.get_snap_list(prefix_string)
        for snap_info in snaps:
            Rbd.remove_rbd_snap_util(self.ceph_conf, self.pool_name, self.rbd_name, snap_info['name'])

    def export_diff(self, start_snap=None, end_snap=None):
        return Rbd.export_rbd_diff_util(self.ceph_conf, self.pool_name, self.rbd_name, start_snap, end_snap)

    def import_diff(self, diff_path):
        return Rbd.import_rbd_diff_util(self.ceph_conf, self.pool_name, self.rbd_name, diff_path)

    @staticmethod
    def diff(ceph_conf_master, ceph_conf_slave, master_rbd_pool_path, slave_rbd_pool_path):
        master_rbd = Rbd(ceph_conf_master, master_rbd_pool_path["pool_name"], master_rbd_pool_path['rbd_name'])
        slave_rbd = Rbd(ceph_conf_slave, slave_rbd_pool_path["pool_name"], slave_rbd_pool_path['rbd_name'])

        startTs = time.time()
        Logger().info("Starting diff %s/%s" % (master_rbd.pool_name, master_rbd.rbd_name))

        master_snaps = master_rbd.get_snap_list(GlobalVar.snap_name_prefix)
        slave_snaps = slave_rbd.get_snap_list(GlobalVar.snap_name_prefix)

        Logger().info("rbd %s/%s master snaps : %s" % (master_rbd.pool_name, master_rbd.rbd_name, repr(master_snaps)))
        Logger().info("rbd %s/%s slave snaps : %s" % (master_rbd.pool_name, master_rbd.rbd_name, repr(slave_snaps)))

        end_snap = GlobalVar.snap_name_prefix + str(GlobalVar.now_ts)
        if len(master_snaps) < 1:
            #第一次备份，在主rbd创建快照，删除备rbd所有快照，导出主rbd创建以来的差异
            slave_rbd.clean_snaps(GlobalVar.snap_name_prefix)
            start_snap = None
        elif len(slave_snaps) < 1:
            master_rbd.clean_snaps(GlobalVar.snap_name_prefix)
            slave_rbd.clean_snaps(GlobalVar.snap_name_prefix)
            start_snap = None
        else:
            master_last_snap = master_snaps[-1]
            for slave_snap_info in reversed(slave_snaps):
                if slave_snap_info['name'] != master_last_snap['name']:
                    slave_rbd.remove_snap(slave_snap_info['name'])
            slave_snaps = slave_rbd.get_snap_list(GlobalVar.snap_name_prefix)
            if len(slave_snaps) < 1: #全部清了
                start_snap = None
            else:
                start_snap = master_last_snap['name']

        master_rbd.create_snap(end_snap)
        res1, res2, diff_path = master_rbd.export_diff(start_snap, end_snap)
        if res1 != 0:
            raise Exception("export diff %s/%s failed! %s" % (master_rbd.pool_name, master_rbd.rbd_name, repr(res2)))
        Logger().debug("exported diff_path: " + diff_path)
        slave_rbd.import_diff(diff_path)

        #清理旧快照
        master_snaps = master_rbd.get_snap_list(GlobalVar.snap_name_prefix)[:-1]
        slave_snaps = slave_rbd.get_snap_list(GlobalVar.snap_name_prefix)[:-1]
        for snap_info in master_snaps:
            master_rbd.remove_snap(snap_info['name'])
        for snap_info in slave_snaps:
            slave_rbd.remove_snap(snap_info['name'])

        mysql = MySql(Logger())
        str_sql = "INSERT INTO `BackupInfo` (CephClusterID, PoolName, RbdName, LastSnapName) " \
                  "VALUES (1, '%s', '%s', '%s')  ON DUPLICATE KEY UPDATE LastSnapName='%s'" % \
                  (master_rbd.pool_name, master_rbd.rbd_name, end_snap, end_snap)
        mysql.execute(str_sql)

        Logger().info("Complete diff %s/%s cost %s(s)" % (master_rbd.pool_name, master_rbd.rbd_name, str(time.time()-startTs)))

    @staticmethod
    def import_rbd_diff_util(ceph_conf, pool_name, rbd_name, diff_path):
        if not Util.exist_file(diff_path):
            raise Exception("exported diff file %s unfound" % (diff_path, ))

        output = subprocess.Popen(" ".join(['rbd --id admin', '-c', ceph_conf['conffile'],
                                            ' --keyring', ceph_conf['keyring'],
                                            'import-diff', diff_path, pool_name+"/"+rbd_name]),
                                  shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        Logger().debug(" ".join(['rbd --id admin', '-c', ceph_conf['conffile'],
                                                    ' --keyring', ceph_conf['keyring'],
                                                    'import-diff', diff_path, pool_name+"/"+rbd_name]))

        lines = "\n".join(output.stdout.readlines())
        if lines.find("Error EINVAL") != -1:
            returnCode = -1
            raise Exception((returnCode, lines))
        else:
            returnCode = 0
        return returnCode, lines

    @staticmethod
    def export_rbd_diff_util(ceph_conf, pool_name, rbd_name, start_snap=None, end_snap=None):
        # 2）导出某个image从创建到某快照时刻的变化
        # 例如，将pool/image1创建时和快照snap1的差量保存至image1s1diff文件
        # rbd export-diff pool/image1@snap1 image1s1diff
        # 3）导出某个image此刻和某快照时刻的变化
        # 例如，将现在pool/image1和快照snap1的差量保存至image1s1diff2文件
        # rbd export-diff pool/image1 --from-snap snap1 image1s1diff2
        path_name = "/tmp/%s/%s/%s/" % (str(GlobalVar.now_ts), pool_name, rbd_name)
        Util.rmdirs(path_name)
        Util.mkdir_p(path_name)
        if not start_snap:
            output = subprocess.Popen(" ".join(['rbd --id admin', '-c', ceph_conf['conffile'],
                                                ' --keyring', ceph_conf['keyring'],
                                                'export-diff', pool_name+"/"+rbd_name+"@"+end_snap,
                                                path_name+end_snap]),
                                      shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            Logger().debug(" ".join(['rbd --id admin', '-c', ceph_conf['conffile'],
                            ' --keyring', ceph_conf['keyring'],
                            'export-diff', pool_name+"/"+rbd_name+"@"+end_snap,
                            path_name+end_snap]))
        else:
            # rbd export-diff --from-snap snap1 source-pool/image1@snap2
            output = subprocess.Popen(" ".join(['rbd --id admin', '-c', ceph_conf['conffile'],
                                                ' --keyring', ceph_conf['keyring'],
                                                'export-diff', "--from-snap", start_snap,
                                                pool_name+"/"+rbd_name+"@"+end_snap, path_name+end_snap]),
                                      shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            Logger().debug(" ".join(['rbd --id admin', '-c', ceph_conf['conffile'],
                            ' --keyring', ceph_conf['keyring'],
                            'export-diff', "--from-snap", start_snap,
                            pool_name+"/"+rbd_name+"@"+end_snap, path_name+end_snap]))

        lines = "\n".join(output.stdout.readlines())
        if lines.find("Error EINVAL") != -1:
            return_code = -1
            raise Exception((return_code, lines))
        else:
            return_code = 0
        return return_code, lines, path_name+end_snap

    @staticmethod
    def remove_rbd_snap_util(ceph_conf, pool_name, rbd_name, snap_name):
        with CephUtils.connect_cluster(ceph_conf, pool_name) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                with rbd.Image(ioctx, rbd_name) as image:
                    if image.is_protected_snap(snap_name):
                        image.unprotect_snap(snap_name)
                    image.remove_snap(snap_name)

    @staticmethod
    def create_rbd_snap_util(ceph_conf, pool_name, rbd_name, snap_name):
        with CephUtils.connect_cluster(ceph_conf, pool_name) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                with rbd.Image(ioctx, rbd_name) as image:
                    image.create_snap(snap_name)

    @staticmethod
    def get_rbd_snap_list_util(ceph_conf, pool_name, rbd_name):
        snap_name_list = []
        with CephUtils.connect_cluster(ceph_conf, pool_name) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                with rbd.Image(ioctx, rbd_name) as image:
                    snap_iterator = image.list_snaps()
                    for snap_dict in snap_iterator:
                        snap_name_list.append(snap_dict)
        return snap_name_list

    @staticmethod
    def remove_rbd_util(ceph_conf, pool_name, rbd_name):
        with CephUtils.connect_cluster(ceph_conf, pool_name) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                rbd_inst = rbd.RBD()
                rbd_inst.remove(ioctx, rbd_name)

    @staticmethod
    def create_rbd_util(ceph_conf, pool_name, rbd_name, size_byte):
        with CephUtils.connect_cluster(ceph_conf, pool_name) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                rbd_inst = rbd.RBD()
                rbd_inst.create(ioctx, rbd_name, size_byte)  # bytes

    @staticmethod
    def get_rbd_size_byte_util(ceph_conf, pool_name, rbd_name):
        with CephUtils.connect_cluster(ceph_conf, pool_name) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                with rbd.Image(ioctx, rbd_name) as image:
                    rbd_size = int(image.size())  # B
        return rbd_size

