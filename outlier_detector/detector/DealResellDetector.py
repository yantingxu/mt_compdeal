#!/usr/bin/env python
#coding=utf-8

import logging
from Detector import Detector

##############################################
# 面向ResellTask的Detector
##############################################
class DealResellDetector(Detector):

    def __init__(self):
        super(DealResellDetector, self).__init__()
        # 一定要写wiki
        self._wiki = None

    def detect(self, task):
        if self._wiki is None:
            raise Exception("亲, 不写WIKI不让执行的...")

        # 区分新老数据，并使用老数据建立索引
        logging.info("Building Index...")
        deals_to_detect, deals_to_build = self._split(task)
        deal_index = self._build_deal_index(deals_to_build)
        logging.info("Done.")

        # 检测各个新的deal
        logging.info("Detecting each deal...")
        resell_count = 0
        for deal in deals_to_detect:
            if self.filter(deal):
                resell_info = self._detect_deal(deal, deal_index)
                if resell_info:
                    resell_count += 1
                    task.fill(self.__class__.__name__, deal['dealid'], resell_info)
                    #logging.debug("CATCH IT: %s" % str([deal['dealid'], resell_info]))
        logging.info("Totally %d Resellings are catched!" % resell_count)

    def _split(self, task):
        """ 新老数据拆分 """
        deals_to_detect = []
        deals_to_build = []
        for deal in task.get_problem():
            if deal['isnew']:
                deals_to_detect.append(deal)
            else:
                deals_to_build.append(deal)
        return deals_to_detect, deals_to_build

    def _build_deal_index(self, task):
        """ 为老数据建立索引"""
        pass

    def _detect_deal(self, deal, deal_index):
        """ 具体的检测逻辑 """
        raise Exception("DETECT_DEAL!!")




