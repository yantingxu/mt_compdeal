#!/usr/local/bin/python
#coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import abc

class Writer(object) :
    __metaclass__ = abc.ABCMeta

    def __init__(self, **params) :
        pass

    @abc.abstractmethod
    def before_write(self) :
        pass

    @abc.abstractmethod
    def after_write(self) :
        pass

    @abc.abstractmethod
    def write(self) :
        pass

