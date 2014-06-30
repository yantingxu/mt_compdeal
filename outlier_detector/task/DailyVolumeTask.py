#!/usr/bin/env python
#coding=utf-8
 
import types
import datetime

from config import TASK_CONFIG
from task import Task
import json
import logging
import datetime
import traceback

#######################################################
# Deal在指定时间范围内天级别销量异常检测
# 每个deal各天的销量都在检测范围之内，但是策略可以选择只检测最近一天的数据
# 例如，AbnormalDailyVolume所有天都检测，而QuantityIncrease等只检测最近的一天
#######################################################
class DailyVolumeTask(Task):
 
    def _prepare_sql(self):
        template_sql = """
        """
        return template_sql


    def _prepare_params(self):
        name = self._task_name
        date_delta = TASK_CONFIG[name]['PARAMS']['DATE_DELTA']
        date_start = self._date - datetime.timedelta(date_delta)
        date_end = self._date
        websiteids = "','".join(TASK_CONFIG[name]['PARAMS']['WEBSITES'])
        return {'today': self._date, 'date_start': date_start, 'date_end': date_end, 'websiteids': websiteids}


    def get_date_range(self):
        name = self._task_name
        date_delta = TASK_CONFIG[name]['PARAMS']['DATE_DELTA']
        return (self._date - datetime.timedelta(date_delta), self._date)


    def _prepare_writer(self):
        from writer.DailyVolumeDetectResult import DailyVolumeDetectResult
        writer = DailyVolumeDetectResult()
        return writer


    def _write_to_result_table(self, writer, solution):
        header = ['detect_date', 'date', 'dealid', 'strategy', 'extra', 'estimate_volume', 'volume']
        lines = []
        for detect_key in solution:
            dealid, date = detect_key
            for strategy in solution[detect_key]:
                result = solution[detect_key][strategy]
                format_line = [self._date, date, dealid, strategy, json.dumps(result['extra']), result['estimate_volume'], result['volume']]
                lines.append(format_line)

        if not lines:
            return 

        logging.debug(lines[0])
        writer.write(self._date, header, lines)


if __name__ == '__main__':

    # DailyVolumeTask
    today = datetime.date.today() - datetime.timedelta(1)
    task_name = 'ROUTINE_DAILY_VOLUME_DETECT'
    task = DailyVolumeTask(task_name, today, True)
    task.prepare_problem()
    task.fill('TEST_STRATEGY', (1, 1), ['whatever'])
    task.fill('TEST_STRATEGY', (2, 1), ['whatever'])
    c = NullCombiner()
    task.persist(c)
