#!/usr/bin/python
# -*- coding:utf-8 -*-
# made by likunxiang

from Common.GlobalVar import GlobalVar

import MySQLdb
import traceback


class MySql:
    conn = None
    cursor = None

    def __init__(self, logger=None, charset='utf8'):
        self.conn_error = True
        self.host = GlobalVar.host
        self.user = GlobalVar.user
        self.passwd = GlobalVar.passwd
        self.db = GlobalVar.dbname
        self.charset = charset
        self.cursor = None
        self.logger = logger
        self.__myconnect__()

    def __myset_conn_error(self, e):
        if e.args[0] == 2006 or e.args[0] == 2003:
            self.conn_error = True

    def __myconnect__(self):
        try:
            self.logger.debug('host[%s], user[%s], passwd[%s], db[%s]' % (self.host, self.user, "xxx", self.db))
            if self.db=='':
                if self.cursor:
                    self.cursor.close()
                if self.conn:
                    self.conn.close()
                self.conn = MySQLdb.connect(self.host,self.user,self.passwd,port=3306)
            else:
                if self.cursor:
                    self.cursor.close()
                if self.conn:
                    self.conn.close()
                self.conn = MySQLdb.connect(self.host,self.user,self.passwd,self.db,port=3306)
            self.conn_error= False
        except MySQLdb.Error,e:
            self.logger.error('Cannot connect to server\nERROR: ' + repr(e))
            self.conn_error= True
            self.cursor = None
            self.logger.error(traceback.format_exc())
            #raise Exception("Database configure error!!!")
            return
        self.cursor = self.conn.cursor()
        self.cursor.execute("SET NAMES utf8")

    def execute(self, sql, value=None):
        try:
            if self.conn_error:
                self.__myconnect__()
            self.cursor.execute(sql,value)
            return self.conn.commit()
        except MySQLdb.Error, e:
            self.logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.__myset_conn_error(e)
            self.logger.error(traceback.format_exc())
        except Exception,e:
            self.logger.error("Exception: " + repr(e))
            self.logger.error(traceback.format_exc())

    def __myclose__(self):
        """ Terminate the connection """
        try:
            if self.conn:
                self.conn.close()
            if self.cursor:
                self.cursor.close()
        except MySQLdb.Error,e:
            self.logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.logger.error(traceback.format_exc())
        except Exception,e:
            self.logger.error("Exception: " + repr(e))
            self.logger.error(traceback.format_exc())
        finally:
            self.conn_error = True

    def __del__(self):
        """ Terminate the connection """
        self.__myclose__()
