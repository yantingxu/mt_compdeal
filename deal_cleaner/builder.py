#!/usr/bin/env python
#coding=utf-8

import abc
from compdeal_config import *
from compdeal_utils import *

###########################
# 中间产品类: 更新策略类
###########################
class Builder:

    __metaclass__ = abc.ABCMeta

    def __init__(self, current_time):
        self._partition_key = None
        self._source_attrs = {}
        self._current_time = current_time

    def reset(self, partition_key, deals):
        # dealid => deal_obj
        self._partition_key = partition_key
        self._source_attrs = deals

    @abc.abstractmethod
    def build(self, product):
        """ 利用源数据计算得到目标数据
            1. calculate _target_attrs from _source_attrs
            2. could not actually change __target_attrs but record it in product
               * product.update_deal()
               * product.update_attr(), e.g. for CompdealLinkage
            3. product.add_callback (if side effect exists)
        """
        raise Exception('Implement Me!!')

    def __callback(self, *args):
        """ 额外操作，Load进目标表之前执行
        """
        pass

    def __repr__(self):
        return str(self._source_attrs)


class TestBuilder(Builder):
    def build(self, product):
        pass
