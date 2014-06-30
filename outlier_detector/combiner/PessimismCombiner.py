#!/usr/bin/env python
#coding=utf-8

from combiner import Combiner

class PessimismCombiner(Combiner):
    """ 悲观主义：所有detector都认为其异常时才认定是异常"""

    def get_combined(self, solution):
        """ 实际上就是NullCombiner"""
        return solution

