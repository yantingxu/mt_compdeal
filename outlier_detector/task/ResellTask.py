#!/usr/bin/env python
#coding=utf-8
 
import types
import datetime
import json

from config import TASK_CONFIG
from task import Task

#######################################################
# Resell 重复上线单检测
#######################################################
class ResellTask(Task):
 
    def _prepare_sql(self):
        template_sql = """
        """
        return template_sql
 

    def _prepare_params(self):
        name = self._task_name
        date_delta = TASK_CONFIG[name]['PARAMS']['DATE_DELTA']
        enddate_start = self._date - datetime.timedelta(date_delta)
        enddate_end = self._date
        begindate_start = self._date
        websiteids = "','".join(TASK_CONFIG[name]['PARAMS']['WEBSITES'])
        revenue = TASK_CONFIG[name]['PARAMS']['CURRENT_REVENUE']
        return {'enddate_start': enddate_start, 'enddate_end': enddate_end, 'begindate_start': begindate_start, 'websiteids': websiteids, 'history_revenue': revenue}


    def _prepare_writer(self):
        from writer.ResellDetectResult import ResellDetectResult
        writer = ResellDetectResult()
        return writer

    def _write_to_result_table(self, writer, solution):
        header = ['detect_date', 'old_dealid', 'dealid', 'strategy', 'basenumber', 'extra']
        lines = []
        for detect_key in solution:
            dealid = detect_key
            for strategy in solution[detect_key]:
                result = solution[detect_key][strategy]
                format_line = [self._date, result['old_dealid'], dealid, strategy, result['basenumber'], json.dumps(result)]
                lines.append(format_line)

        self._write_raw(writer, lines, header)

if __name__ == '__main__':

    # ResellTask
    today = datetime.date.today() - datetime.timedelta(1)
    task_name = 'ROUTINE_DEAL_RESELL_DETECT'
    task = ResellTask(task_name, today, True)
    task.prepare_problem()
    task.fill('TEST_STRATEGY', (1, 1), ['whatever'])
    task.fill('TEST_STRATEGY', (2, 1), ['whatever'])
    c = NullCombiner()
    task.persist(c)





