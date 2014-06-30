#!/usr/bin/env python
#coding=utf-8

import logging
import traceback
import sys
import datetime
from optparse import OptionParser
from config import TASK_CONFIG, LOG_CONFIG
from utils.topology import Topology
 
#########################################################
# 任务执行器: 封装整个检测逻辑, 只需要指明任务名和必要参数
#########################################################
class TaskPerformer:
 
    def __init__(self, name, today):
        self.__config = TASK_CONFIG[name]       # 读取任务配置
        is_debug = self.__config['DEBUG']       # 是否为调试模式

        # LOGGING_CONFIG
        LOG_CONFIG['level'] = logging.INFO if not is_debug else logging.DEBUG
        LOG_CONFIG['filename'] = "log/%s.log" % name
        logging.basicConfig(**LOG_CONFIG)

        logging.info("============= TASK [%s] ============" % name)
        logging.info("LOGGING CONFIG INIT DONE")

        # INIT_TASK
        self.__init_task(name, today, is_debug)
        logging.info("TASK INIT DONE: %s" % self.__task)

        # INIT_DETECTORS
        self.__init_detectors()
        logging.info("DETECTORS INIT DONE: %s" % self.__detectors)


    def __init_task(self, name, today, is_debug):
        """ 初始化任务 """
        task_type = self.__config['TYPE']
        exec "from task.%s import %s" % (task_type, task_type)
        exec "task = %s(name, today, is_debug)" % task_type
        self.__task = task


    def __init_detectors(self):
        """ 初始化Detectors"""
        detector_names = self.__config['DETECTORS'].keys()
        detectors = {}.fromkeys(detector_names, None)
        for name in detector_names:
            exec "from detector.%s import %s" % (name, name)
            exec "detectors[name] = %s()" % name
        self.__detectors = detectors
 
        self.__topology = Topology()
        for name in detector_names:
            parents = self.__config['DETECTORS'][name]
            self.__topology.add(name, parents)


    def run(self):
        """ 搞起 """
        logging.info("PREPARE PROBLEM...")
        self.__task.prepare_problem()
        logging.info("DONE.")

        logging.info("DETECT STARTS...")
        for name in self.__topology:
            logging.info("[%s]'s TURN NOW..." % name)
            detector = self.__detectors[name]
            try:
                detector.detect(self.__task)
                logging.info("DONE.")
            except Exception, tx:
                logging.error("DETECTOR %s GOES WRONG: %s" % (name, str([tx, traceback.format_exc()])))
        logging.info("DETECT ENDS")


    def persist(self):
        """ 持久化 """
        combiner_class = self.__config['COMBINER']
        exec "from combiner.%s import %s" % (combiner_class, combiner_class)
        exec "combiner = %s()" % combiner_class
        self.__task.persist(combiner)


# 参数解析
def set_usage():
    parser = OptionParser(usage='usage: python %prog task_name [-d] ...')
    parser.add_option("-d", "--delta", dest = "delta_day", type = 'int', default = 1, help = "set day interval before today")
    params, args = parser.parse_args()

    exec "params = %s" % str(params) # cause type of var params return from prase_args() is instance, not a dict
    if len(args) == 0:
        task_name = 'ROUTINE_DAILY_VOLUME_DETECT'
    else:
        task_name = args[0]
    delta_day = params.get('delta_day', 1)
    today = datetime.date.today() - datetime.timedelta(days = delta_day)

    return task_name, today


if __name__ == '__main__':
    task_name, today = set_usage()
    #task_name = 'ROUTINE_DEAL_RESELL_DETECT'
    #task_name = 'ROUTINE_DAILY_VOLUME_DETECT'
    #print task_name, today

    p = TaskPerformer(task_name, today)
    p.run()
    p.persist()


