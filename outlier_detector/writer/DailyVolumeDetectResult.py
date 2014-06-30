#!/usr/local/bin/python
#coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import traceback
import datetime
import logging
from DetectResultWriter import DetectResultWriter

class DailyVolumeDetectResult(DetectResultWriter):
    """ 面对DailyVolumeTask的Writer类"""

    def _get_table_ddl(self):
        RESULT_TABLE_DDL = '''
            CREATE TABLE `%s` (
              `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
              PRIMARY KEY(`id`),
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='异常检测结果集';
        ''' % self._table_name
        return RESULT_TABLE_DDL

    def _get_table_name(self):
        RESULT_TABLE_NAME = 'compdeal_check_result'
        #RESULT_TABLE_NAME = 'daily_volume_detect_result'
        return RESULT_TABLE_NAME

    def _get_table_fields(self):
        RESULT_RECORD_COLUMN = set(['date', 'dealid', 'strategy', 'extra', 'estimate_volume', 'volume', 'detect_date'])
        return RESULT_RECORD_COLUMN

    def _get_db_meta(self):
        from config import TASK_CONFIG, DB_CONFIG
        is_debug = TASK_CONFIG['ROUTINE_DAILY_VOLUME_DETECT']['DEBUG']
        meta_name = 'test' if is_debug else 'cis'
        meta = DB_CONFIG[meta_name]
        return meta


    def _get_repeat_id(self, writer, data, header):
        """
        得到重复数据
        """
        id_set = set()

        sql_select_format = \
            "select id from " + self._table_name + \
            """ 
            where 
                `detect_date` = '%s' 
                and `date` = '%s' 
                and `strategy` = '%s' 
                and `dealid` = %s
            """

        # get index
        detect_date_index = header.index('detect_date')
        date_index = header.index('date')
        strategy_index = header.index('strategy')
        dealid_index = header.index('dealid')

        for item in data:
            sql_select = sql_select_format % (item[detect_date_index], item[date_index], item[strategy_index], item[dealid_index])
            result = None
            try:
                result = writer.query(sql_select)
            except Exception, tx:
                logging.error("QUERY DAILYVOLUME REPEAT ID  GOES WRONG: %s" % str([tx, traceback.format_exc()]))

            if result:
                id_set.add(str(result[0][0]))
        return id_set



