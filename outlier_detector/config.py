#!/usr/bin/env python
#coding=utf-8
 
import logging
import types
import codecs
 
##############################################################
# 任务配置: 任务类型，所需要的数据，及需要参与各任务的Detectors
#############################################################
TASK_CONFIG = {
    'ROUTINE_DAILY_VOLUME_DETECT' : {
        'TYPE': 'DailyVolumeTask',
        'PARAMS': {
            'DATE_DELTA': 20,
            'WEBSITES': ['lashou', 'dianping', 'nuomi', '55tuan', 'meituan', 'baidu', 'didatuan', 'guilinlife', '0750tuan', 'taobao', '0543', 'zg163', 'hosotuan', 'aipin'],
        },
        'DETECTORS': {
            # detector => [parent_0, parent_1, ...]
            'QuantityIncrease': [],
            'QuantityIncreaseDaily': ['QuantityIncrease', ],
            'HighPrice': ['QuantityIncreaseDaily', ],
            'VolumeStartHigh': ['HighPrice', ],
            'AbnormalDailyVolume': ['VolumeStartHigh', ],
        },
        'COMBINER': 'NullCombiner',
        'DEBUG': False,
    },

    'ROUTINE_DEAL_RESELL_DETECT' : {
        'TYPE': 'ResellTask',
        'PARAMS': {
            'DATE_DELTA': 180,
            'WEBSITES': ['lashou','360buy', 'nuomi','meituan','dianping','55tuan', 'baidu','didatuan','guilinlife','0750tuan', 'taobao', '0543'],
            'CURRENT_REVENUE': 3000,
        },
        'DETECTORS': {
            # detector => [parent_0, parent_1, ...]
            'NaiveResellDetector': [],
        },
        'COMBINER': 'NullCombiner',
        'DEBUG': False,
    },
}

##############################################################
# 日志配置
#############################################################
LOG_CONFIG = {
    'level' : logging.INFO,
    'format' : '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    'datefmt' : '%Y-%m-%d %H:%M:%S',
    'filename' : 'log/default.log',
    'filemode' : 'a',
}

##############################################################
# 字段解析
#############################################################
class FieldParser:
    @staticmethod
    def daily_volumes_parser(line):
        if not line:
            return {}

        daily_volumes = {}
        sublines = eval(line)
        for subline in sublines:
            try:
                date, volume = subline.split(":")
            except:
                continue
            volume = float(volume)
            if volume < 0:
                continue
            if date in daily_volumes:
                daily_volumes[date] = max(volume, daily_volumes[date])
            else:
                daily_volumes[date] = volume

        return daily_volumes

    @staticmethod
    def daily_addtimes_parser(line):
        if not line:
            return {}

        daily_addtimes = {}
        sublines = eval(line)
        for subline in sublines:
            try:
                date, addtime = subline.split("|||")
            except:
                continue
            if date in daily_addtimes:
                daily_addtimes[date] = max(addtime, daily_addtimes[date])
            else:
                daily_addtimes[date] = addtime 

        return daily_addtimes

    @staticmethod
    def citylist_parser(line):
        if not line:
            return []
        citylist = eval(line)
        if 9999 in citylist:
            citylist.remove(9999)
        return citylist


FIELD_PARSER = {
    'dealid': (types.IntType, ),
    'websiteid': (types.StringType, ),
    'url': (types.StringType, ),
    'date': (types.StringType, ),
    'identity': (lambda x: codecs.encode(x, 'utf-8'), ),
    'price': (types.FloatType, ),
    'value': (types.FloatType, ),
    'title': (lambda x: codecs.encode(x, 'utf-8'), ),
    'cityid': (types.IntType, ),
    'typeid': (types.IntType, ),
    'revenue': (types.FloatType, ),
    'volume': (types.IntType, ),
    'begintime': (types.IntType, ),
    'endtime': (types.IntType, ),
    'status': (types.IntType, ),
    'daily_volumes': (FieldParser.daily_volumes_parser, ),
    'raw_volumes': (FieldParser.daily_volumes_parser, ),
    'isnew': (types.IntType, ),
    'begintime': (types.IntType, ),
    'endtime': (types.IntType, ),
    'curnumber': (types.IntType, ),
    'firstcurnumber': (types.IntType, ),
    'ratio': (types.FloatType, ),
    'total_volume': (types.IntType, ),
    'all_daily_volumes': (FieldParser.daily_volumes_parser, ),
    'real_volumes': (FieldParser.daily_volumes_parser, ),
    'addtimes': (FieldParser.daily_addtimes_parser, ),
    'categoryid': (types.IntType, ),
    'classid': (types.IntType, ),
    'isallcity': (types.IntType, ),
    'citylist': (FieldParser.citylist_parser, ),
    'begindate': (types.StringType, ),
}


##############################################################
# 字段解析
#############################################################
DB_CONFIG = {
    #线下测试库
    'test' : {
        'host' : '',
        'port' : ,
        'db' : '',
        'user' : '',
        'passwd' : '',
    },
}





