# 项目背景
该项目是用于对一个主ceph集群里面的rbd镜像进行diff差异备份到另外一个备ceph集群，当主ceph集群rbd数据丢失或者损坏需要恢复的时候，
可以借助备ceph集群的备份数据进行恢复。ceph本身自带了rbd mirror功能可以实现rbd同步，但是我们测试过，开启了mirror功能之后性能
非常差，完全满足不了商用的需求，所以我们就实现了一个基于diff的定时rbd备份功能的项目。

# 使用方法
- 基础配置  
  在运行程序的服务器，需要安装rbd和mysql相关的python开发库，可以使用pip安装。  
  用到的库有：MySQLdb、rados、rbd，如果运行有缺库的自行安装即可  
  
- ceph集群配置  
  把主集群的配置文件放到 ```/etc/ceph_snap_diff/master```  
  把备集群的配置文件放到 ```/etc/ceph_snap_diff/slave```  
  
- 数据库配置  
  数据库表格sql文件 ceph_diff_snap_all.sql 导入到程序可以访问的数据库即可，使用的数据库名称是 ceph_diff  
  
- 运行程序  
  在需要备份的时间点直接运行程序就会进行实时备份，用户可以根据自身需求来制定执行机制，比如放crontab定时执行  
  // -s 是对非“."或非"default"的pool进行异地备份  
  // n 是代表同时对n个rbd镜像进行备份，也是代表线程个数  
  ```python main.py -s n```  

# 开发人员
@坤爷
