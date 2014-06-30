#!/usr/bin/env python
#coding=utf-8

####################################################################
# Detector类: 仅实现检测策略
# 使用filter来获得每个子类关注的instance, 检测结果写回到task中
####################################################################
class Detector(object):
 
    def __init__(self):
        self._params = {}

    def __repr__(self):
        return self.__class__.__name__
 
    def filter(self, deal, *args, **kwargs):
        raise Exception('Implement Me!!')
 
    def detect(self, task, *args, **kwargs):
        raise Exception('Implement Me!!')

if __name__ == '__main__':
    d = Detector()




