#!/usr/bin/env python
#coding=utf-8

import MySQLdb
import re
import numpy as np
import math
from compdeal_config import db_meta, source, target

#######################
# 统计工具函数
#######################
class StatTools:
    @staticmethod
    def MeanAndStddev(vector):
        if not vector:
            return None, None
        else:
            mean_val = np.mean(vector)
            l = len(vector)
            std_val = ((np.std(vector)**2)*max(1, l)/max(1, l-1))**0.5
            return mean_val, std_val

    @staticmethod
    def FiveNumSummary(vector):
        minval = min(vector)
        q1 = np.percentile(vector, 25)
        median = np.percentile(vector, 50)
        q3 = np.percentile(vector, 75)
        maxval = max(vector)
        return minval, q1, median, q3, maxval

    @staticmethod
    def Naive_FiveNumSummary(vector):
        if len(vector) <= 1:
            return []

        minval = min(vector)
        median = np.percentile(vector, 50)
        maxval = max(vector)

        vector = sorted(vector)
        l = len(vector)
        q1 = vector[int(math.ceil((l+1)/4.0))-1]
        q3 = vector[int(math.floor(3.0*(l+1)/4.0))-1]
        return minval, q1, median, q3, maxval

#######################
# 单例数据库连接
#######################
class Connector:

    __instance = {}

    def __init__(self):
        raise Exception('This is a Singleton DB Connector!!!')

    @classmethod
    def getInstance(cls, name = 'CIS'):
        if cls.__instance.get(name, None) is None:
            if name in db_meta:
                cls.__instance[name] = MySQLdb.connect(**db_meta[name])
        return cls.__instance.get(name, None)

    @classmethod
    def close(cls):
        for name in cls.__instance.keys():
            cls.__instance[name].close()
            del cls.__instance[name]

