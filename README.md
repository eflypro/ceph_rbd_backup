# ceph_diff_snap
该项目是用于对一个主ceph集群里面的rbd镜像进行diff差异备份到另外一个备ceph集群，当主ceph集群rbd数据丢失或者损坏需要恢复的时候，
可以借助备ceph集群的备份数据进行恢复。ceph本身自带了rbd mirror功能可以实现rbd同步，但是我们测试过，开启了mirror功能之后性能
非常差，完全满足不了商用的需求，所以我们就实现了一个基于diff的定时rbd备份功能的项目。
