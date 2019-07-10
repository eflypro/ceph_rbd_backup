/*
SQLyog Ultimate v12.5.0 (32 bit)
MySQL - 5.7.24-0ubuntu0.16.04.1 : Database - ceph_diff
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`ceph_diff` /*!40100 DEFAULT CHARACTER SET latin1 */;

USE `ceph_diff`;

/*Table structure for table `BackupInfo` */

DROP TABLE IF EXISTS `BackupInfo`;

CREATE TABLE `BackupInfo` (
  `BackupInfo_ID` int(11) NOT NULL AUTO_INCREMENT,
  `CephClusterID` int(11) NOT NULL,
  `PoolName` varchar(256) NOT NULL,
  `RbdName` varchar(256) NOT NULL,
  `LastSnapName` varchar(256) NOT NULL,
  `TrigTs` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`BackupInfo_ID`),
  UNIQUE KEY `uniq_rbd` (`PoolName`,`RbdName`),
  KEY `FK_Relationship_1` (`CephClusterID`),
  CONSTRAINT `FK_Relationship_1` FOREIGN KEY (`CephClusterID`) REFERENCES `CephCluster` (`CephClusterID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*Data for the table `BackupInfo` */

/*Table structure for table `CephCluster` */

DROP TABLE IF EXISTS `CephCluster`;

CREATE TABLE `CephCluster` (
  `CephClusterID` int(11) NOT NULL AUTO_INCREMENT,
  `ClusterName` varchar(256) NOT NULL DEFAULT 'default',
  PRIMARY KEY (`CephClusterID`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;

/*Data for the table `CephCluster` */

insert  into `CephCluster`(`CephClusterID`,`ClusterName`) values 
(1,'default');

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
