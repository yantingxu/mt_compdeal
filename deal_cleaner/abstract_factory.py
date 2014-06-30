#!/usr/bin/env python
#coding=utf-8

import abc
from data_loader import *
from compdeal_linkage import CompdealLinkage
from compdeal_mark import CompdealMark
from compdeal_type import CompdealType

########################################################
# 中间产品抽象工厂类, 每个产品系列包括
#  1. DataLoader子类: 数据抽取
#  2. Buider子类: 更新策略
########################################################
class AbstractFactory(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, current_time):
        self._data = None
        self._builder = None
        self._current_time = current_time

    @abc.abstractmethod
    def get_data(self):
        pass

    @abc.abstractmethod
    def get_builder(self):
        pass


class CompdealLinkageFactory(AbstractFactory):

    def __init__(self, current_time):
        super(CompdealLinkageFactory, self).__init__(current_time)

    def get_data(self):
        if self._data is None:
            self._data = LinkageLoader(self._current_time)
        return self._data

    def get_builder(self):
        if self._builder is None:
            self._builder = CompdealLinkage(self._current_time)
        return self._builder


class CompdealMarkFactory(AbstractFactory):

    def __init__(self, current_time):
        super(CompdealMarkFactory, self).__init__(current_time)

    def get_data(self):
        if self._data is None:
            self._data = MarkLoader(self._current_time)
        return self._data

    def get_builder(self):
        if self._builder is None:
            self._builder = CompdealMark(self._current_time)
        return self._builder


class CompdealTypeFactory(AbstractFactory):

    def __init__(self, current_time):
        super(CompdealTypeFactory, self).__init__(current_time)

    def get_data(self):
        if self._data is None:
            self._data = TypeLoader(self._current_time)
        return self._data

    def get_builder(self):
        if self._builder is None:
            self._builder = CompdealType(self._current_time)
        return self._builder

