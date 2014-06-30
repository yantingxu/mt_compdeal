#!/usr/local/bin/python
#coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
from Reader import Reader

class MysqlReader(Reader) :
    """Mysql 获取数据"""

    meta_need = ['host', 'port', 'user', 'passwd', 'db']

    def __init__(self, **meta) :
        self.meta = {}
        super(Reader, self).__init__()
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

    def _is_readonly(self, sql) :
        sql = sql.lower()
        keywords = ['insert ', 'update ', 'drop ', 'truncate ', 'alter ', 'delete ', 'create ']
        for item in keywords :
            if item in sql :
                return False

        return True

    def before_read(self) :
        pass

    def after_read(self) :
        pass

    def read_raw(self, sql, return_dict = True):
        if not self.connect :
            self._connect()

        if return_dict:
            self.cursor = self.connect.cursor(cursorclass = MySQLdb.cursors.DictCursor)
        else:
            self.cursor = self.connect.cursor()

        if not self._is_readonly(sql):
            raise Exception('reader can only read data, no others')
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        self.cursor.close()
        return result

    def read(self, sql, return_dict = True ) :
        self.before_read()
        result = self.read_raw(sql, return_dict)
        self.after_read()
        return result

