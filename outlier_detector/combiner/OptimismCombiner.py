#!/usr/bin/env python
#coding=utf-8

from combiner import Combiner

class OptimismCombiner(Combiner):
    """ 乐观主义：任意一个detector认为是异常就认定是异常"""

    def get_combined(self, solution):
        """ 实际上就是NullCombiner"""
        return solution

