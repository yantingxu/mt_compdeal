#!/usr/local/bin/python
#coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import abc

class Reader(object) :
    __metaclass__ = abc.ABCMeta

    def __init__(self, **params) :
        pass

    @abc.abstractmethod
    def before_read(self) :
        pass

    @abc.abstractmethod
    def after_read(self) :
        pass

    @abc.abstractmethod
    def read(self) :
        pass

