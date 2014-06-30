#!/usr/bin/env python
#coding=utf-8

from combiner import Combiner

class ConfidenceCombiner(Combiner):
    """ 取Confidence最大的detector的检测结果"""
    
    def get_combined(self, solution):
        """ 实际上就是NullCombiner"""
        return solution

