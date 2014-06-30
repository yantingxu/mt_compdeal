#!/usr/bin/env python
#coding=utf-8
 
import logging
import types
import sys
import os
import codecs
import time
import traceback
from collections import defaultdict
from config import TASK_CONFIG
from combiner.combiner import Combiner

#############################################################################
# 任务类: 对problem和solution的封装, 支持持久化写入结果表中
# Problem - 大部分原始数据抽取逻辑集中在这里, 包括deal的各个属性
# Solution - 各个Detector的检测结果也集中这里
############################################################################
class Task(object):
 
    def __init__(self, name, today, is_debug = False):
        self._task_name = name
        self._date = today
        self._problem = {}                        # DealModels
        self._solution = defaultdict(dict)        # detect_key => {strategy: [...]}; e.g. detect_key = (date, dealid) for DailyVolumeTask
        self._is_debug = is_debug

    def __repr__(self):
        return "%s-%s %d" % (self._task_name, self._date, len(self._problem))

    def get_problem(self):
        return self._problem

    def get_today(self):
        return self._date

    ################ READ RAW DATA FOR DETECT ####################
    def prepare_problem(self):
        """ 准备待检测数据, 形成Problem """
        if not self._is_debug:
            # load data by sql
            items = self._get_data_from_sql()
        else:
            # debug mode: use csv_reader to read data from file
            items = self._get_data_from_csv()

        # parse the result
        items = self.parse(items)

        # fill in self._problem
        self._problem = items
        logging.info("PROBLEM: %s" % self)

    def _get_data_from_csv(self):
        """ 从文件中读取数据, 供调试时使用 """
        from reader.CsvReader import CsvReader
        reader = CsvReader()
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'debug/%s.data' % self._task_name)
        logging.debug("CSV_FILE: %s" % filename)

        if os.path.exists(filename):
            data = reader.read(filename)
            return data
        else:
            # 自动生成测试文件
            data = self._get_data_from_sql()
            if not data:
                logging.debug("No Data from Hive")
                return
            header = data[0].keys()
            from writer.CsvWriter import CsvWriter
            writer = CsvWriter()
            writer.write(data, filename, header)
            return data

    def _get_data_from_sql(self):
        """ 从数据库读取数据 """
        template_sql = self._prepare_sql()
        params = self._prepare_params()
        sql = template_sql % params
        logging.debug(sql)
        #sys.exit()

        # read from hive
        reader = self._prepare_reader()
        data = reader.read(sql)
        logging.debug(len(data))
        return data

    def _prepare_sql(self):
        """ SQL与具体的任务类型相关(overidden) """
        raise Exception('Implement Me!!')
 
    def _prepare_params(self):
        """ Params与具体的任务相关（任务类型＋配置）(overidden) """
        raise Exception('Implement Me!!')

    def _prepare_reader(self):
        """ 定义在哪个db上执行sql, 默认为HIVE(overidden) """
        from reader.HiveReader import HiveReader
        from config import DB_CONFIG
        reader = HiveReader(**DB_CONFIG['hive'])
        return reader
 
    def parse(self, items):
        """SQL字段解析"""
        from config import FIELD_PARSER
        for item in items:
            try:
                for field in item:
                    raw_data = item[field]
                    item[field] = FIELD_PARSER[field][0](raw_data)
            except Exception, e:
                logging.error("PARSER WRONG %s: %s" % (item, str([e, traceback.format_exc()])))
        return items

    ################ FILL IN SOLUTION ####################
    def fill(self, strategy, detect_key, detect_result):
        """ 策略的检测结果存回task """
        self._solution[detect_key][strategy] = detect_result

    def get_solution(self, detect_key = None, strategy = None):
        """ 获取当前的检测结果, 下游策略可以参考上游策略的检测结果 """
        if detect_key is None:
            return self._solution
        if detect_key not in self._solution:
            return None
        if strategy is None:
            return self._solution[detect_key]
        else:
            return self._solution[detect_key].get(strategy, None)


    ################## PERSIST INTO DB TABLE ####################
    def persist(self, combiner = None):
        """ 将solution持久化写到结果表中 """
        # 如果需要将各策略的结果融合则实现在combiner中
        if isinstance(combiner, Combiner):
            combined_solution = combiner.get_combined(self._solution)

        # 写回结果表
        try:
            writer = self._prepare_writer()
            self._write_to_result_table(writer, combined_solution)
            logging.info("PERSIST DONE: %d" % len(self._solution))
        except Exception, tx:
            logging.error("PERSIST FAILED: %s" % str([tx, traceback.format_exc()]))

    def _prepare_writer(self):
        """ 写到啥db里去？"""
        raise Exception('Implement Me!!')
 
    def _write_to_result_table(self, writer, line):
        """ 以什么格式写？"""
        raise Exception('Implement Me!!')

    def _write_raw(self, writer, lines, header):
        """ 写操作 """
        if self._is_debug:
            from writer.CsvWriter import CsvWriter
            csv_writer = CsvWriter()
            filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'debug/%s_result_%s.data' % (self._task_name, time.strftime("%Y%m%d%H%M%S", time.localtime())))
            logging.debug("CSV_RESULT_FILE: %s" % filename)
            csv_writer.write(lines, filename, header, False)
        writer.write(self._date, header, lines)

