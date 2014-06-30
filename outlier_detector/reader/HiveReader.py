#!/usr/local/bin/python
#coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from OpenHiveClient import *
from MysqlReader import MysqlReader

class HiveReader(MysqlReader) :
    """Hive 获取数据"""

    meta_need = ['host', 'port', 'username']

    def _connect(self) :
        if self.connect :
            return
        self.connect = OpenHiveClient(**self.meta)

    def _is_readonly(self, sql) :
        return True

    def read_raw(self, sql, return_dict = True) :
        if not self.connect :
            self._connect()

        query = Query()
        query.query = sql
        query.username = self.meta['username']

        self.connect.query(query)
        result = self.connect.fetchAll()

        #解析line
        data = []
        header = self.connect.header
        for line in result:
            line = line.decode('utf-8', 'ignore')
            sub_items = line.strip().split('\t')
            if return_dict:
                line_dict = {}
                for index, key in enumerate(header):
                    value = sub_items[index]
                    line_dict[key] = value
                data.append(line_dict.copy())
            else:
                data.append(sub_items)

        return data

