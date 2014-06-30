#!/usr/bin/env python
#coding=utf-8

import logging
import datetime
import numpy as np
import traceback
from Detector import Detector
from utils.utils import Utils

####################################################################
# 销量预测方法类: 作为成员变量供DailyVolumeDetector使用
####################################################################
class VolumeEstimator(object):
    def __init__(self):
        self._data = {}

    def _prepare_data(self, *args, **kwargs):
        pass

    def estimate(self, deal, current_date, *args, **kwargs):
        return None


class LinearVolumeEstimator(VolumeEstimator):

    def estimate(self, deal, current_date, history_length):
        """ 使用当天deal历史销量拟合直线估计当天销量
        """
        daily_volumes = filter(lambda x:x[1] >= 0, deal['all_daily_volumes'].items())
        daily_volumes.sort(key = lambda x:x[0])
        earlist_date = current_date - datetime.timedelta(days = history_length)
        history_records = [item for item in daily_volumes if str(earlist_date) <= item[0] < str(current_date)]
        #logging.debug("DAILY_VOLUMES: %s; EARLY: %s; RECORD: %s" % (daily_volumes, earlist_date, history_records))
        if len(history_records) < 2:
            # 想画条线至少需要两个点
            return None

        xs = [(datetime.datetime.strptime(item[0], '%Y-%m-%d').date() - earlist_date).days for item in history_records]
        ys = [item[1] for item in history_records]
        x = (current_date - earlist_date).days
        linear_fit_volume = max(Utils.get_linear_estimate(xs, ys, x), 1)
        #logging.debug("X: %s, Y: %s, x: %s, fit: %s" % (xs, ys, x, linear_fit_volume))

        return linear_fit_volume


class HistoryMedianVolumeEstimator(VolumeEstimator):

    def estimate(self, deal, current_date, history_length):
        """ 使用当前deal历史销量中位数估计当天销量
        """
        earlist_date = current_date - datetime.timedelta(days = history_length)
        smooth_daily_volumes = filter(lambda x: str(earlist_date) <= x[0] < str(current_date), filter(lambda x:x[1] >= 0, deal['real_volumes'].items()))
        smooth_volume_set = [item[1] for item in smooth_daily_volumes]

        if smooth_volume_set:
            #estimate_volume = np.percentile(smooth_volume_set, 50) * deal['ratio']
            #logging.debug("DEALID: %d; VOLUMES: %s; SMOOTH_VOLUMES: %s; MEDIAN: %s; RATIO: %s; ESTIMATE: %s" % (deal['dealid'], deal['real_volumes'], smooth_volume_set, np.percentile(smooth_volume_set, 50), deal['ratio'], estimate_volume))
            estimate_volume = np.percentile(smooth_volume_set, 50)
            #logging.debug("DEALID: %d; VOLUMES: %s; SMOOTH_VOLUMES: %s; MEDIAN: %s; ESTIMATE: %s" % (deal['dealid'], deal['real_volumes'], smooth_volume_set, np.percentile(smooth_volume_set, 50), estimate_volume))
        else:
            estimate_volume = 0

        return estimate_volume



class HistoryMeanVolumeEstimator(VolumeEstimator):

    def estimate(self, deal, current_date, history_length):
        """ 使用当前deal历史销量均值作为当天销量
        """
        earlist_date = current_date - datetime.timedelta(days = history_length)
        smooth_daily_volumes = filter(lambda x: str(earlist_date) <= x[0] < str(current_date), filter(lambda x:x[1] >= 0, deal['real_volumes'].items()))
        smooth_volume_set = [item[1] for item in smooth_daily_volumes]

        if smooth_volume_set:
            estimate_volume = np.mean(smooth_volume_set)
            #logging.debug("DEALID: %d; VOLUMES: %s; SMOOTH_VOLUMES: %s; MEDIAN: %s; ESTIMATE: %s" % (deal['dealid'], deal['real_volumes'], smooth_volume_set, np.percentile(smooth_volume_set, 50), estimate_volume))
        else:
            estimate_volume = 0

        return estimate_volume


class CityAvgVolumeEstimator(VolumeEstimator):

    def __init__(self):
        super(CityAvgVolumeEstimator, self).__init__()
        self._aux_config = {
            'compdeal_num_max' : 1000.0,        # 计算compdeal_num的最大值
            'allcity_coefficient' : 15000,      # 若是全国单，则乘以系数
            'national_regular' : {u'((\d\d))店通用' : 10},  # 若是没有检查出来的全国单，直接阈值为10
        }

    def update_aux_config(self, config):
        self._aux_config.update(config)

    def is_prepared(self):
        return self._data

    def set_history_range(self, history_range):
        self._prepare_data(history_range)

    def _prepare_data(self, date_range):
        """ 城市历史离线数据
        """
        begindate, enddate = date_range
        interval = 1.0*(enddate-begindate).days
        city_stat = Utils.get_city_stat(date_range)

        _data = {}
        for websiteid in city_stat:
            _data[websiteid] = {}
            for cityid in city_stat[websiteid]:
                dealnum, revenue = city_stat[websiteid][cityid]
                if not dealnum:
                    dealnum = self._aux_config['compdeal_num_max']
                avg_dealnum = dealnum / interval
                #if avg_dealnum > self._aux_config['compdeal_num_max']:
                #    avg_dealnum = self._aux_config['compdeal_num_max']
                _data[websiteid][cityid] = (avg_dealnum, revenue)

        self._data = _data


    def estimate(self, deal):
        """ deal所在城市销量历史均值作为当天销量
        """
        avg_revenue = self.__get_avg_city_revenue(deal)
        avg_dealnum = self.__get_avg_city_dealnum(deal)
        if avg_revenue is not None and avg_dealnum:
            avg_deal_revenue = avg_revenue / avg_dealnum
            avg_deal_volume = avg_deal_revenue / deal['price']
        else:
            avg_deal_volume = None

        return avg_deal_volume


    def __get_avg_city_revenue(self, deal):
        """ deal所在城市历史期间交易额均值
            若是全国单，则按该全国单所涉及的城市的销售额的和计算阈值
        """
        city_stat = self._data
        if not city_stat:
            raise Exception("City_Stat is not ready!!")

        websiteid = deal['websiteid']
        if websiteid not in city_stat:
            return None

        cityid = deal['cityid']
        if cityid in city_stat[websiteid] and not self._is_national_deal(deal):
            deal_city_revenue = city_stat[websiteid][cityid][1]
        else:
            citylist = deal['citylist']
            deal_city_revenue = sum(city_stat[websiteid].get(cityid, [0.0, 0.0])[1] for cityid in citylist)
            allcity_revenue = self._aux_config['compdeal_num_max'] * self._aux_config['allcity_coefficient']
            # 由于全国单关联城市经常出错，所以，设置一个最小值，容错
            if deal_city_revenue < allcity_revenue:
                deal_city_revenue = allcity_revenue

        return deal_city_revenue


    def _is_national_deal(self, deal):
        """ 判断deal是否为全国单
        """
        return deal['isallcity'] == 1 or \
            Utils.check_regular(deal['title'], self._aux_config['national_regular']) is not None


    def __get_avg_city_dealnum(self, deal):
        """ deal所在城市历史期间展位数均值
        """
        city_stat = self._data
        if not city_stat:
            raise Exception("City_Stat is not ready!!")

        websiteid = deal['websiteid']
        if websiteid not in city_stat:
            return None

        cityid = deal['cityid']
        avg_dealnum = self._aux_config['compdeal_num_max']
        if cityid != 9999 and cityid in city_stat[websiteid]:
            avg_dealnum = city_stat[websiteid][cityid][0]

        return avg_dealnum



# Decorators
def check_extra_format(func):
    """ 约束extra格式(used as a decorator): 至少要有raw_data和standard """
    def prepare_and_check_extra(self, deal, *args, **kwargs):
        # 取得extra数据
        extra = func(self, deal, *args, **kwargs)
        # 必须是dict格式
        if type(extra) != dict:
            raise Exception("Invalid Format for Extra: Must be a dict")
        # raw_data用于存储被检测的天销量序列，即[[date, volume], ...]
        if 'raw_data' not in extra or not isinstance(extra['raw_data'], (tuple, list)):
            raise Exception("Invalid Format for Extra['raw_data']")
        # standard用于存储判断标准
        if 'standard' not in extra:
            raise Exception("Invalid Format for Extra['standard']")
        return extra

    return prepare_and_check_extra


####################################################################
# 面向DailyVolumeTask的Detector
# QuantityIncrease, HighPrice, VolumeStartHigh只针对deal+today的检测
# AbnormalDailyVolume则对deal+every_date_in_range做检测
####################################################################
class DailyVolumeDetector(Detector):

    def __init__(self):
        super(DailyVolumeDetector, self).__init__()
        # 一定要写wiki
        self._wiki = None

        # 默认的检测流程是使用_detect_estimator来确定阈值，用_smooth_estimator来抹平销量；非强制，可以使用其它方式
        self._detect_estimator = None   # 销量预测方法实例：用于确定阈值
        self._smooth_estimator = None   # 销量预测方法实例: 用于确定抹平销量值

    def detect(self, task):
        if self._wiki is None:
            raise Exception("亲, 不写WIKI不让执行的...")

        outlier_count = 0
        deals_to_detect = task.get_problem()
        context = self._get_detect_context(task)
        for deal in deals_to_detect:
            try:
                if self.filter(deal, context):
                    outliers = self._detect_deal(deal, context)
                    outlier_count += len(outliers)
                    for detect_key in outliers:
                        task.fill(self.__class__.__name__, detect_key, outliers[detect_key])
                        #logging.debug("CATCH IT: %s" % str([detect_key, outliers[detect_key]]))
            except Exception, tx:
                logging.error("DEAL %s GOES WRONG: %s" % (deal, str([tx, traceback.format_exc()])))
        logging.info("Totally %d outliers are catched!" % outlier_count)

    def _get_detect_context(self, task):
        """ 获取本次检测所需要的上下文信息 """
        history_range = task.get_date_range()
        parents = task.get_solution()
        today = task.get_today()
        return {'history': history_range, 'parents': parents, 'today': today}

    # 如果采用非默认的检测流程，可以不鸟后两个方法
    def _detect_deal(self, deal, context):
        """ 具体的检测流程 """
        raise Exception("DETECT_DEAL!!")

    def _get_detect_threshold(self, deal, context, *args, **kwargs):
        """ 使用_detect_estimator及辅助逻辑确定检测阈值 """
        return None

    def _get_smooth_volme(self, deal, context, *args, **kwargs):
        """ 使用_smooth_estimator及辅助逻辑确定抹平销量值 """
        return None

    @check_extra_format
    def _prepare_extra(self, deal, *args, **kwargs):
        """ 判断过程中的中间结果，包括原始数据，阈值等 """
        return None



