#!/usr/bin/env python
import sys
import rados
import rbd
import traceback
import time
#import libvirt
from xml.dom import minidom
import xml.etree.ElementTree as ET
import uuid
import time
import commands

cephConf = '/etc/ceph_snap_diff/master/ceph.conf'
cephPath = '/etc/ceph_snap_diff/master/ceph.client.admin.keyring'
id = 'admin'

def build_start_time():
    tt = time.localtime(time.time()-60)
    ss = "%d-%02d-%02d %02d:%02d:%02d"%(tt.tm_year, tt.tm_mon, tt.tm_mday, tt.tm_hour, tt.tm_min, 0)
    ts = time.strptime(ss, "%Y-%m-%d %H:%M:%S")
    start_time = int(time.mktime(ts))
    return start_time

def clone_system_image():
    global cephconf, cephpath, id

    before_time = time.time()

    try:

        pool_name = 'libvirt-pool'
        parent_image_name = 'fe28d3da-2e49-11e8-94fb-525400f27a88.raw'
        parent_image_snap_name = 'snap'
        disk_uuid = str(uuid.uuid1())
        child_image_name = disk_uuid + '.raw'
        print child_image_name
        with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id ) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                rbd_inst = rbd.RBD()

                with rbd.Image(ioctx, parent_image_name) as image:
                    if not image.is_protected_snap(parent_image_snap_name):
                        image.protect_snap(parent_image_snap_name)

                rbd_inst.clone(ioctx, parent_image_name, parent_image_snap_name, ioctx, child_image_name)
                #with rbd.Image(ioctx, child_image_name) as child_image:
                #    child_image.flatten()
    except rbd.PermissionError, e:
        print e
    except rbd.IOError, e:
        print e

    print time.time() - before_time
    print child_image_name

def clone_disk():

    before_time = time.time()

    cephConf = '/etc/ceph/KVM-P001/libvirt/ceph.conf'
    cephPath = '/etc/ceph/KVM-P001/libvirt/ceph.client.libvirt.keyring'
    id = 'libvirt'
    pool_name = 'libvirt'

    parent_image_name = 'Template1.0_Ubuntu_17.10_64.raw'
    child_image_name = '20180206019111111225.raw'
    snap_name = child_image_name
    print 'test clone'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id ) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:

            with rbd.Image(ioctx, parent_image_name) as image:
                image.create_snap(snap_name)
                image.protect_snap(snap_name)

            rbd_inst = rbd.RBD()
            rbd_inst.clone(ioctx, parent_image_name, snap_name, ioctx, child_image_name)

            with rbd.Image(ioctx, child_image_name) as child_image:
                child_image.flatten()

            with rbd.Image(ioctx, parent_image_name) as image:
                image.unprotect_snap(snap_name)
                image.remove_snap(snap_name)

    print time.time() - before_time

def copy_disk():

    before_time = time.time()

    cephConf = '/etc/ceph/KVM-P001/libvirt/ceph.conf'
    cephPath = '/etc/ceph/KVM-P001/libvirt/ceph.client.libvirt.keyring'
    id = 'libvirt'
    pool_name = 'libvirt'

    parent_image_name = 'Template1.0_Ubuntu_17.10_64.raw'
    child_image_name = '2018020601923123301356.raw'
    print 'test copy'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id ) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            with cluster.open_ioctx(pool_name) as dest_ioctx:
                with rbd.Image(ioctx, parent_image_name) as image:
                    image.copy(dest_ioctx, child_image_name)

    print time.time() - before_time


def get_pool_image():
    global cephconf, cephpath, id

    before_time=time.time()
    pool_name = 'test_pool'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            rbd_inst = rbd.RBD()
            image_name_list = rbd_inst.list(ioctx)
            print image_name_list
    print time.time() - before_time

def _snapshot_disk():
    global cephconf, cephpath, id

    print 'begin'
    before_time = time.time()

    pool_name = 'libvirt-pool'
    image_name = 'D-180509110003.raw'
    snap_name = str(build_start_time())
    snap_name = snap_name + '11223'
    child_image_name = 'test5.raw'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            with rbd.Image(ioctx, image_name) as image:
                image.create_snap(snap_name)
                image.protect_snap(snap_name)
            rbd_inst = rbd.RBD()
            rbd_inst.clone(ioctx,image_name,snap_name,ioctx,child_image_name)
            with rbd.Image(ioctx, child_image_name) as child_image:
                child_image.flatten()
            with rbd.Image(ioctx, image_name) as image:
                image.unprotect_snap(snap_name)
                image.remove_snap(snap_name)

    print time.time() - before_time
    print child_image_name



def create_snap_4():
    global cephconf, cephpath, id

    diskxml = """
    <disk type="network" device='disk'>
        <source protocol="rbd" name="libvirt-pool/20180205002.raw">
            <host name='192.168.3.31' port='6789'/>
            <host name='192.168.3.32' port='6789'/>
            <host name='192.168.3.30' port='6789'/>
        </source>
        <auth username='libvirt'>
            <secret type='ceph' uuid='c035aa33-d5ef-45dd-9d72-33b1e5fba115'/>
        </auth>
        <target bus="virtio" dev="hda"></target>
    </disk>
    """
    pool_name = 'libvirt-pool'
    image_name = '20180205002.raw'
    snap_name = str(build_start_time())
    child_image_name = '20180205013.raw'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            rbd_inst = rbd.RBD()
            rbd_inst.rename(ioctx, image_name, child_image_name)
            with rbd.Image(ioctx, child_image_name) as image:
                image.create_snap(snap_name)
                image.protect_snap(snap_name)
            rbd_inst.clone(ioctx,child_image_name,snap_name,ioctx,image_name)

            try:
                conn = libvirt.open(None)
                if conn == None:
                    print('Failed to connect')
                name = 'test.vm.07'
                dom = conn.lookupByName(name)
            except libvirt.libvirtError:
                print("Failed to connect to the hypervisor")
                sys.exit(1)

            before_time = time.time()
            #detach disk
            try:
                dom.detachDeviceFlags(diskxml, libvirt.VIR_DOMAIN_AFFECT_CURRENT)
            except:
                print 'detach error'

            #check detach griginal disk

            try:
                start_time = time.time()
                timeout = 120
                while time.time() - start_time < timeout:
                    raw_xml = dom.XMLDesc(0)
                    if check_disk(raw_xml, pool_name + '/' + image_name) == False:
                        break
                    time.sleep(0.5)
                    print 'has old disk'
            except libvirt.libvirtError:
                print("Domain %s is not running" % name)
                sys.exit(0)

            #attach disk
            #try:
                #dom.attachDeviceFlags(diskxml, libvirt.VIR_DOMAIN_AFFECT_CURRENT)
            #except:
                #print 'attach error'

            conn.close()

    print time.time() - before_time
    print image_name
    print child_image_name

def check_disk(raw_xml, image):
    print image
    xml = minidom.parseString(raw_xml)
    diskTypes = xml.getElementsByTagName('disk')
    for diskType in diskTypes:
        print('disk: type=' + diskType.getAttribute('type') + ' device = ' + diskType.getAttribute('device'))
        diskNodes = diskType.childNodes
        for diskNode in diskNodes:
            if diskNode.nodeName[0:1] != '#':
                for attr in diskNode.attributes.keys():
                    if diskNode.attributes[attr].name == 'name' and diskNode.attributes[attr].value == image:
                        print image
                        return True
    return False

def list_children():
    global cephconf, cephpath, id

    before_time = time.time()

    pool_name = 'libvirt-pool'
    image_name = 'Template3.2_Ubuntu_16.04_64_v1.0.raw'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            with rbd.Image(ioctx, image_name) as image:
                snaps =  image.list_snaps()
                for snap in snaps:
                    image.set_snap(snap['name'])
                    print snap['name']
                    children_list = image.list_children()
                    for children in children_list:
                        child_image_name = children[1]
                        uuid = child_image_name.split('.raw')[0]
                        print uuid
                    image.set_snap(None)

    print time.time() - before_time

def delete_image():
    global cephconf, cephpath, id

    before_time = time.time()

    pool_name = 'libvirt-pool'
    image_name = '184630c1-265f-11e8-8dee-801844ea18e0.raw'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            with rbd.Image(ioctx, image_name) as image:
                snaps =  image.list_snaps()
                for snap in snaps:
                    image.set_snap(snap['name'])
                    children_list = image.list_children()
                    for children in children_list:
                        child_image_name = children[1]
                        print child_image_name
                        with rbd.Image(ioctx, child_image_name) as child_image:
                            child_image.flatten()
                    image.set_snap(None)
                    if image.is_protected_snap(snap['name']):
                        image.unprotect_snap(snap['name'])
                    image.remove_snap(snap['name'])

            rbd_inst = rbd.RBD()
            rbd_inst.remove(ioctx, image_name)

    print time.time() - before_time
    print image_name

def upprotect_snap():
    global cephconf, cephpath, id

    before_time = time.time()

    pool_name = 'libvirt-pool'
    image_name = '20180202015.raw'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            with rbd.Image(ioctx, image_name) as image:
                snaps =  image.list_snaps()
                for snap in snaps:
                    if image.is_protected_snap(snap['name']):
                        image.unprotect_snap(snap['name'])
    print time.time() - before_time
    print image_name

def rename_image():
    global cephconf, cephpath, id

    before_time = time.time()

    pool_name = 'libvirt-pool'
    src_name = '20180205002.raw'
    dest_name = '20180205012.raw'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            rbd_inst = rbd.RBD()
            rbd_inst.rename(ioctx, src_name, dest_name)

    print time.time() - before_time
    print image_name
    print child_image_name

def create_data_disk():
    global cephconf, cephpath, id

    before_time = time.time()

    size = 1000 * 1024**3   #1000GiB
    print size
    #disk_uuid = '%s' % (uuid.uuid1())
    disk_name = 'D-180620160010' + '.raw'

    try:
        pool_name = 'libvirt-pool'
        with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                rbd_inst = rbd.RBD()
                rbd_inst.create(ioctx, disk_name, size)
    except rbd.ImageExists:
        print traceback.format_exc()
        print 'ImageExists'

    print time.time() - before_time
    print disk_name

def list_image():
    global cephconf, cephpath, id

    pool_name = 'libvirt-pool'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            rbd_inst = rbd.RBD()
            list = rbd_inst.list(ioctx)
            print list

def list_image_and_snap():
    global cephconf, cephpath, id

    pool_name = 'libvirt-pool'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            rbd_inst = rbd.RBD()
            list = rbd_inst.list(ioctx)
            for image_image in list:
                print image_image
                with rbd.Image(ioctx, image_image) as image:
                    snaps = image.list_snaps()
                    for snap in snaps:
                        print '.........' + snap['name']

def list_pools():
    global cephconf, cephpath, id

    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        print cluster.get_cluster_stats()
        print cluster.list_pools()
        for pool in cluster.list_pools():
            with cluster.open_ioctx(pool) as ioctx:
                print pool
                print ioctx.get_stats()

                rbd_inst = rbd.RBD()
                try:
                    rbd_list = rbd_inst.list(ioctx)
                except:
                    #traceback.print_exc()
                    continue
                for image_name in rbd_list:
                    print image_name
                    continue


def get_parent():
    global cephconf, cephpath, id

    before_time = time.time()

    pool_name = 'libvirt'
    image_name = 'Template3.2_Ubuntu_16.04_64_clone.raw'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            with rbd.Image(ioctx, image_name) as image:
                try:
                    info = image.parent_info()
                    ceph_pool = info[0]
                    p_image_name = info[1]
                    snap_name = info[2]

                    print ceph_pool
                    print p_image_name
                    print snap_name

                except rbd.ImageNotFound:
                    print 'the '
    print time.time() - before_time
    print image_name

    test1 = 'test'
    test2 = {'111':'11111'}
    if test1 == test2:
        print 'test'
    else:
        print 'test3333'

def list_snap_num():
    global cephconf, cephpath, id

    before_time = time.time()

    pool_name = 'libvirt-pool'
    image_name = 'D-180511110002.raw'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            with rbd.Image(ioctx, image_name) as image:
                snaps = image.list_snaps()
                #print sum(1 for _ in snaps)
                num = 0
                for _ in snaps:
                    num += 1
                print num

    print time.time() - before_time
    print image_name


def list_rbd_lock():
    global cephconf, cephpath, id

    pool_name = 'libvirt-pool'
    image_name = 'D-180704140002.raw'
    with rados.Rados(conffile=cephConf, conf=dict(keyring=cephPath), rados_id=id) as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            with rbd.Image(ioctx, image_name) as image:
                #client, cookie, address
                list_lockers = image.list_lockers()


def list_rbd_watchers():
    global cephconf, cephpath, id

    pool_name = 'libvirt-pool'
    image_name = 'D-180704140002.raw'
    cmd = 'rbd status --id %s --image %s/%s'%(id, pool_name, image_name)
    (status, output) = commands.getstatusoutput(cmd)
    lines = output.split("\n")
    watchers = lines[1:]
    for watcher in watchers:
        info = watcher.split(" ")
        watcher_address = info[0].split("=")[1]
        print watcher_address


def clear_rbd_wathers(id, storage_name, image_name):
    image_name = image_name + '.raw'
    cmd = 'rbd status --id %s --image %s/%s'%(id, storage_name, image_name)
    print cmd
    (status, output) = commands.getstatusoutput(cmd)
    print status, output
    lines = output.split("\n")
    watchers = lines[1:]
    for watcher in watchers:
        info = watcher.split(" ")
        watcher_address = info[0].split("=")[1]

        cmd2 = 'ceph --id %s osd blacklist add %s'%(id, watcher_address)
        print cmd2
        (status, output) = commands.getstatusoutput(cmd2)
        print status, output




get_pool_image()
