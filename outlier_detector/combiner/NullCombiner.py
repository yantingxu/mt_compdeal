#!/usr/bin/env python
#coding=utf-8

import logging
from combiner import Combiner

class NullCombiner(Combiner):
    """ 我是打酱油的，啥都不干"""

    def get_combined(self, solution):
        logging.debug("Running %s" % self.__class__.__name__)
        return solution

