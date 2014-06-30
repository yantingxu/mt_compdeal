#!/usr/local/bin/python
#coding=utf-8

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import datetime
import logging
import traceback
from MysqlWriter import MysqlWriter

###########################################
# 异常检测结果写回DB的封装
# 每个Task对应一个DetectResultWriter的子类
###########################################
class DetectResultWriter(MysqlWriter) :
    """数据写回结果集DB"""

    def __init__(self):
        self._table_name = self._get_table_name()
        self._fields = self._get_table_fields()
        meta = self._get_db_meta()
        super(DetectResultWriter, self).__init__(**meta)

    def _get_table_ddl(self):
        raise Exception("TABLE DDL CANNOT BE EMPTY!!")

    def _get_table_name(self):
        raise Exception("TABLE NAME CANNOT BE EMPTY!!")

    def _get_table_fields(self):
        raise Exception("TABLE FIELDS CANNOT BE EMPTY!!")

    def _get_db_meta(self):
        raise Exception("DB META CANNOT BE EMPTY!!")

    def before_write(self):
        sql = "show tables like '%s'" % self._table_name
        if not self.query(sql) :
            self.write_raw(self._get_table_ddl(), True)

    def _get_repeat_id(self, writer, data, header):
        raise Exception("GET REPEAT ID CANNOT BE EMPTY!!")

    def write(self, detect_date, header = None, data = None) :
        '''一般策略数据写回方法，在策略判定完成之后按照格式配置好数据，传递过来写回db'''
        if not header or not data or not isinstance(data, (list, tuple)):
            return

        sql_clear = "delete from `%s` where `id` in " % self._table_name

        if set(header).difference(self._fields):
            raise Exception('result data format error')
        field_str = "`%s`" % '`,`'.join(header)

        try:
            repeat_ids = set()
            repeat_ids = self._get_repeat_id(super(DetectResultWriter, self), data, header)
        except Exception, tx:
            logging.error("DAILYVOLUME GET REPEAT ID  GOES WRONG: %s" % str([tx, traceback.format_exc()]))

        val_strs = []
        for item in data:
            val_str = "('%s')" % "','".join([str(f) for f in item])
            val_strs.append(val_str)

        sql_insert = "insert into `%s` (%s) values %s" % (self._table_name, field_str, ",".join(val_strs))

        if repeat_ids:
            sql_clear += "(%s)" % ','.join(repeat_ids)
            logging.debug(sql_clear)
            super(DetectResultWriter, self).write(sql_clear, False)
        logging.debug(sql_insert)
        super(DetectResultWriter, self).write(sql_insert, True)
