# -*- coding:utf-8 -*-
import json
import subprocess
from StringIO import StringIO

import rados
import rbd
import commands
import ConfigParser


class CephUtils(object):
    template_snp_name = 'snap'
    be_deleted_tag = '_deleted'
    mon_port = 6789
    conf_path = '/etc/ceph_snap_diff'
    format = '.raw'

    def __init__(self):
        pass

    # def

    #####################conf################################################
    @staticmethod
    def get_ceph_conf(cluster):
        ceph_conf = {'name': cluster,
                     'conffile': '/etc/ceph_snap_diff/' + cluster + '/ceph.conf',
                     'keyring': '/etc/ceph_snap_diff/' + cluster + '/ceph.client.admin.keyring',
                     'user': 'admin'}
        return ceph_conf

    #####################cluster##########################################
    @staticmethod
    def connect_cluster(ceph_conf, pool_name=None):
        if pool_name:
            c = rados.Rados(conffile=ceph_conf['conffile'],
                                  conf=dict(keyring=ceph_conf['keyring']),
                                  rados_id=ceph_conf['user'])
            #c.connect()
        else:
            c = rados.Rados(conffile=ceph_conf['conffile'],
                                  conf=dict(keyring=ceph_conf['keyring']))
            c.connect()
        return c

    @staticmethod
    def get_cluster_stats(ceph_conf):
        """ This function Read usage info about the ceph cluster,
            this tells you total space, space used, space available
        :return: dict - contains the following keys:
                total (int): total space
                used (int): space used
                avail (int): free space available
        """
        stats = {}
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            cluster_stats = cluster.get_cluster_stats()
            stats['total'] = int(cluster_stats['kb'] / 1024 / 1024)  # GB
            stats['used'] = int(cluster_stats['kb_used'] / 1024 / 1024)  # GB
            stats['avail'] = int(cluster_stats['kb_avail'] / 1024 / 1024)  # GB
        return stats

    @staticmethod
    def get_cluster_df(ceph_conf):
        """
        :param ceph_conf:
        :return: dict - contains the following keys:
                stats: cluster stats
                pools: pools stats
        """
        cmd = "ceph -c %s -k %s --id %s df -f json" \
              % (ceph_conf['conffile'], ceph_conf['keyring'], ceph_conf['rados_id'])
        (status, output) = commands.getstatusoutput(cmd)
        return status, output

    #####################monitor##########################################

    @staticmethod
    def get_monitor_hosts(comp_pool, ceph_pool):
        ip_list = []
        conf_file = str('/etc/ceph/' + comp_pool + '/' + ceph_pool + '/ceph.conf')
        config = ConfigParser.ConfigParser()
        config.read(conf_file)
        try:
            mon_host = config.get('global', 'mon_host')
        except ConfigParser.NoOptionError:
            mon_host = config.get('global', 'mon host')
        for ip in mon_host.split(','):
            ip_list.append(ip)
        return ip_list

    #####################osd##########################################

    @staticmethod
    def add_osd_blacklist(ceph_conf, ip):
        # 300day
        time = 25920000
        cmd = "ceph -c %s -k %s --id %s osd blacklist add %s %s" \
              % (ceph_conf['conffile'], ceph_conf['keyring'], ceph_conf['rados_id'], ip, time)
        (status, output) = commands.getstatusoutput(cmd)
        return status, output

    @staticmethod
    def rm_ceph_osd_blacklist(ceph_conf, ip):
        cmd = "ceph -c %s -k %s --id %s osd blacklist rm %s" \
              % (ceph_conf['conffile'], ceph_conf['keyring'], ceph_conf['rados_id'], ip)
        (status, output) = commands.getstatusoutput(cmd)
        return status, output

    ####################pool###########################################
    @staticmethod
    def get_pool_keyring(comp_pool, ceph_pool):
        keyring_file_name = str('ceph.client.' + ceph_pool + '.keyring')
        keyring_file = str('/etc/ceph/' + comp_pool + '/' + ceph_pool + '/' + keyring_file_name)

        tag = 'key ='
        with open(keyring_file, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if line.find(tag) != -1:
                    line = line.strip('\n')
                    key = line.split(tag)[-1]
                    key = key.strip()
        return key

    @staticmethod
    def get_pool_usage_stats(ceph_conf):
        """ This function Get ceph pool usage statistics.
        :param ceph_conf:
        :return: dict - contains the following keys:
                num_bytes (int) - size of pool in bytes
                num_kb (int) - size of pool in kbytes
                num_objects (int) - number of objects in the pool
                num_object_clones (int) - number of object clones
                num_object_copies (int) - number of object copies
                num_objects_missing_on_primary (int) - number of objets missing on primary
                num_objects_unfound (int) - number of unfound objects
                num_objects_degraded (int) - number of degraded objects
                num_rd (int) - bytes read
                num_rd_kb (int) - kbytes read
                num_wr (int) - bytes written
                num_wr_kb (int) - kbytes written
        """

        pool_stats = {}
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                pool_stats = ioctx.get_stats()
        return pool_stats

    @staticmethod
    def get_rbd_pool_stats(ceph_conf):
        """
        Return RBD pool ceph_conf
        :param ceph_conf:
        :return: dict - contains the following keys:
                image_count (int) - image count
                image_provisioned_bytes (int) - image total HEAD provisioned bytes
                image_max_provisioned_bytes (int) - image total max provisioned bytes
                image_snap_count (int) - image snap count
                trash_count (int) - trash image count
                trash_provisioned_bytes (int) - trash total HEAD provisioned bytes
                trash_max_provisioned_bytes (int) - trash total max provisioned bytes
                trash_snap_count (int) - trash snap count
        """

        pool_stats = {}
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                rbd_inst = rbd.RBD()
                pool_stats = rbd_inst.pool_stats_get(ioctx)
        return pool_stats

    @staticmethod
    def get_pool_allocated(ceph_conf):
        allocated_GB = 0
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                rbd_inst = rbd.RBD()
                image_list = rbd_inst.list(ioctx)
                for rbd_image_name in image_list:
                    with rbd.Image(ioctx, str(rbd_image_name)) as image:
                        vdi_size = int(image.size())  # B
                        vdi_size = int(vdi_size / (1024 * 1024 * 1024))  # GB
                        allocated_GB = allocated_GB + vdi_size
        return allocated_GB

    @staticmethod
    def get_pool_replica_size(ceph_conf):
        pass


    ####################rbd image###########################################


    @staticmethod
    def get_rbd_image_parent(ceph_conf, vdi_name):
        """
        if the image doesn’t have a parent,
        :param ceph_conf:
        :param vdi_name:
        :return: if the image doesn’t have a parent,return None
        """
        rbd_image_name = vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                with rbd.Image(ioctx, str(rbd_image_name)) as image:
                    try:
                        info = image.parent_info()
                    except rbd.ImageNotFound:
                        return None
                    parent = {}
                    parent['pool_name'] = info[0]
                    parent['image_name'] = info[1]
                    parent['snapshot_name'] = info[2]

        return parent

    @staticmethod
    def do_clone_rbd_image_from_image(ceph_conf, parent_vdi_name, snap_name, child_vdi_name):
        """ This function will create a new rbd image and a snapshot.
        :param parent_vdi_name: .
        :param snap_name: .
        """
        parent_image_name = parent_vdi_name + CephUtils.format
        child_image_name = child_vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                with rbd.Image(ioctx, str(parent_image_name)) as image:
                    image.create_snap(str(snap_name))
                    image.protect_snap(str(snap_name))
                rbd_inst = rbd.RBD()
                rbd_inst.clone(ioctx, str(parent_image_name), str(snap_name),
                               ioctx, str(child_image_name))

    @staticmethod
    def do_flatten_rbd_image(ceph_conf, vdi_name):
        rbd_image_name = vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                with rbd.Image(ioctx, str(rbd_image_name)) as image:
                    image.flatten()

    @staticmethod
    def do_copy_rbd_image(ceph_conf, vdi_name, dest_ceph_conf, dest_vdi_name):
        """
        Copy the image to another pool or the same pool.
        :param ceph_conf:
        :param vdi_name:
        :param dest_ceph_conf:
        :param dest_vdi_name:
        :return:
        """
        image_name = vdi_name + CephUtils.format
        dest_image_name = dest_vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                with CephUtils.connect_cluster(dest_ceph_conf) as dest_cluster:
                    with dest_cluster.open_ioctx(dest_ceph_conf['pool_name']) as dest_ioctx:
                        with rbd.Image(ioctx, str(image_name)) as image:
                            image.copy(dest_ioctx, dest_image_name)

    @staticmethod
    def do_remove_rbd_image(ceph_conf, vdi_name):
        """
        remove raw format rbd image(eg: xxx.raw)
        :param ceph_conf:
        :param vdi_name:
        :return:
        """
        rbd_image_name = vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                rbd_inst = rbd.RBD()
                rbd_inst.remove(ioctx, str(rbd_image_name))


    @staticmethod
    def do_rename_rbd_image(ceph_conf, vdi_name, dest_vdi_name):
        """
        :param ceph_conf:
        :param vdi_name: the current name of the image
        :param dest_vdi_name: the new name of the image
        :return:
        """
        rbd_image_name = vdi_name + CephUtils.format
        dest_rbd_image_name = dest_vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                rbd_inst = rbd.RBD()
                rbd_inst.rename(ioctx, rbd_image_name, dest_rbd_image_name)

    @staticmethod
    def resize_rbd_image(ceph_conf, vdi_name, image_size):
        """
        :param ceph_conf:
        :param vdi_name:
        :param image_size: unit is bytes.
        :return:
        """
        rbd_image_name = vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                with rbd.Image(ioctx, str(rbd_image_name)) as image:
                    image.resize(image_size)

    @staticmethod
    def read_rbd_image(ceph_conf, rbd_image_name, offset, length):
        """
        :param ceph_conf:
        :param rbd_image_name: eg(heartbeat.A-02-505-07-20180507-033.img)
        :param offset: the offset to start reading at
        :param length: how many bytes to read
        :return:
        """
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                with rbd.Image(ioctx, str(rbd_image_name)) as image:
                    data = image.read(offset, length)
        return data

    ####################rbd SnapIterator###########################################


    @staticmethod
    def do_protect_rbd_image_snap(ceph_conf, vdi_name, snap_name):
        rbd_image_name = vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                with rbd.Image(ioctx, str(rbd_image_name)) as image:
                    if not image.is_protected_snap(str(snap_name)):
                        image.protect_snap(str(snap_name))

    @staticmethod
    def get_rbd_image_snap_count(ceph_conf, vdi_name):
        snap_num = 0
        rbd_image_name = vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                with rbd.Image(ioctx, str(rbd_image_name)) as image:
                    snaps = image.list_snaps()
                    for _ in snaps:
                        snap_num += 1
        return snap_num

    @staticmethod
    def get_ceph_rbd_image_snap_children(ceph_conf, vdi_name, snap_name):
        rbd_image_name = vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                with rbd.Image(ioctx, str(rbd_image_name)) as image:
                    image.set_snap(snap_name)
                    children_list = image.list_children()
                    image.set_snap(None)
        return children_list

    @staticmethod
    def do_clone_rbd_image_from_snap(ceph_conf, parent_vdi_name, snap_name, child_vdi_name):
        parent_image_name = parent_vdi_name + CephUtils.format
        child_image_name = child_vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                rbd_inst = rbd.RBD()
                rbd_inst.clone(ioctx, str(parent_image_name), str(snap_name), ioctx, str(child_image_name))

    @staticmethod
    def do_rollback_ceph_rbd_image_to_snap(ceph_conf, vdi_name, snap_name):
        rbd_image_name = vdi_name + CephUtils.format
        with CephUtils.connect_cluster(ceph_conf) as cluster:
            with cluster.open_ioctx(ceph_conf['pool_name']) as ioctx:
                with rbd.Image(ioctx, str(rbd_image_name)) as image:
                    image.rollback_to_snap(snap_name)

