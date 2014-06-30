#!/usr/bin/env python
#coding=utf-8

import sys
import logging
import re
import numpy as np
from config import TASK_CONFIG

####################################################
# 辅助函数: 需要有一定的通用性，可供所有Detector调用
###################################################
class Utils:
 
    simple_cache = {}  # 类内置cache

    # 线性预测
    @staticmethod
    def get_linear_estimate(xs, ys, x):
        """ 根据(xs, ys)获得拟合直线，并计算x的拟合值
        """
        import numpy as np
        from scipy.optimize import leastsq
 
        # 待拟合的函数，x是变量，p是参数
        def fun(x, p):
            a, b = p
            return a*x + b
 
        # 计算真实数据和拟合数据之间的误差，p是待拟合的参数，x和y分别是对应的真实数据
        def residuals(p, x, y):
            return fun(x, p) - y
 
        x_array = np.array(xs)
        y_array = np.array(ys)
        r = leastsq(residuals, [1, 1], args=(x_array, y_array))
        calc_value = r[0][0] * x + r[0][1]
 
        return calc_value


    @staticmethod
    def five_num_summary(vector):
        """ 获取向量的five_num_summary
        """
        minval = min(vector)
        q1 = np.percentile(vector, 25)
        median = np.percentile(vector, 50)
        q3 = np.percentile(vector, 75)
        maxval = max(vector)
        return minval, q1, median, q3, maxval


    @staticmethod
    def check_regular(text, regex_list):
        """ 检查text中是否有命中regex_list
        """
        for special_regular in regex_list:
            if re.search(special_regular, text):
                return special_regular
        return None


    # 各策略阈值
    @classmethod
    def get_strategy_config(cls, strategy = None):
        def _get_db_meta():
            from config import TASK_CONFIG, DB_CONFIG
            is_debug = TASK_CONFIG['ROUTINE_DAILY_VOLUME_DETECT']['DEBUG']
            meta_name = 'test' if is_debug else 'cis'
            meta = DB_CONFIG[meta_name]
            return meta

        def prepare_strategy_config():

            from reader.MysqlReader import MysqlReader
            meta = _get_db_meta()
            reader = MysqlReader(**meta)
            sql = ""
            data = reader.read(sql)

            strategy_config = {}
            for record in data:
                strategy_config[record['strategy']] = strategy_config.get(record['strategy'], {})
                strategy_config[record['strategy']][record['typeid']] = float(record['threshold'])
            return strategy_config

        main_key = 'strategy_config'
        if main_key not in cls.simple_cache:
            config = prepare_strategy_config()
            cls.simple_cache[main_key] = config

        if main_key not in cls.simple_cache:
            return None

        if strategy is None:
            return cls.simple_cache[main_key]

        return cls.simple_cache[main_key].get(strategy, None)


    # 城市近期统计数据(TO COMPARE WITH DEAL)
    @classmethod
    def get_city_stat(cls, date_range, websiteid = None, cityid = None):
        """ Invoked by HighPrice and VolumeStartHigh"""
        def retrieve():
            sql = """
            """ % date_range
            logging.debug("CITY_AVG: %s" % sql)

            if TASK_CONFIG['ROUTINE_DAILY_VOLUME_DETECT']['DEBUG']:
                import os
                from reader.CsvReader import CsvReader
                reader = CsvReader()
                filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'debug/city_stat')
                if os.path.exists(filename):
                    logging.debug("CSV_FILE: %s" % filename)
                    data = reader.read(filename)
                    return data
                else:
                    print "File %d does not exist!" % filename
                    sys.exit()
            else:
                # prepare reader and items
                from reader.HiveReader import HiveReader
                from config import DB_CONFIG
                reader = HiveReader(**DB_CONFIG['hive'])
                items = reader.read(sql)
                logging.debug("Items: %d" % len(items))
                return items


        def calc_city_stat():
            # get data from hive/mysql
            # date_range => websiteid => cityid => (dealnum, revenue)
            city_stat = {}
            items = retrieve()
            #logging.debug("ITEM: %s" % items[0])
            for item in items:
                cityid = int(item['cityid'])
                websiteid = item['websiteid']
                revenue = float(item['revenue'])
                dealnum = int(item['dealnum'])
                if websiteid not in city_stat:
                    city_stat[websiteid] = {}
                city_stat[websiteid][cityid] = (dealnum, revenue)
            #logging.debug('CITY_STAT: %d' % len(city_stat))
            return city_stat

        main_key = 'city_stat'
        if (main_key, date_range) not in cls.simple_cache:
            city_stat = calc_city_stat()
            cls.simple_cache[(main_key, date_range)] = city_stat
        if websiteid is None:
            return cls.simple_cache[(main_key, date_range)]
        if cityid is None:
            return cls.simple_cache[(main_key, date_range)][websiteid]

        return cls.simple_cache[(main_key, date_range)][websiteid][cityid]


if __name__ == '__main__':

    title = "【新亚汽车站】新亚驾校                          仅售3020元，价值3120元的C1驾证轿车班单人培训课程！师资力量强大，培训设施齐全！全国百家品牌驾校！让驾照轻松到手！"
    str_regular = {u'(((\d\d))|十)人' : 5, u'驾校' : 5, u'自助':5, u"充值" : 10, u'话费' : 10, u'储值卡' : 10,u'代金券' : 10, u'加油卡':10}                           #若是包含特殊字符串，则把阈值乘以相应参数
    print Utils.check_regular(title.decode('utf-8'), str_regular)

