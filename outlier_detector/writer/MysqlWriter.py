#!/usr/local/bin/python
#coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
from Writer import Writer


class MysqlWriter(Writer) :
    """Mysql 数据写回"""

    meta_need = ['host', 'port', 'user', 'passwd', 'db']

    def __init__(self, **meta) :
        self.meta = {}
        super(Writer, self).__init__()
        for item in self.meta_need :
            if item not in meta.keys() :
                raise Exception('meta not enough')
            self.meta[item] = meta[item]

        self.meta['charset'] = meta.get('charset', 'utf8')

        self.connect = None

    def _connect(self) :
        if self.connect :
            return
        self.connect = MySQLdb.connect(**self.meta)
        #强制不自动commit
        self.connect.autocommit(0)

    def before_write(self) :
        pass

    def after_write(self) :
        pass

    def write(self, sql, iscommit = False) :
        self.before_write()
        self.write_raw(sql, iscommit)
        self.after_write()

    def write_raw(self, sql, iscommit = False) :
        self._connect()
        self.cursor = self.connect.cursor()
        try :
            self.cursor.execute(sql)
        except Exception, e:
            self.connect.rollback()
            raise Exception(str(e))
        else :
            if iscommit :
                self.connect.commit()
        self.cursor.close()

    def query(self, sql) :
        self._connect()
        self.cursor = self.connect.cursor()
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        self.cursor.close()

        return result





