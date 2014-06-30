#!/usr/bin/env python
#coding=utf-8

from collections import defaultdict
 
#############################################################################
# Detectors之间的依赖关系, 实现为Iterator, 可依次返回当前应该执行的Detector
# 先使用add方法添加依赖关系(prepared)，再通过next返回当前结果
# 简单实现，没考虑有环啥的
############################################################################
class Topology:
 
    def __init__(self):
        self.__topology = defaultdict(set)
        self.__indegree = {}
        self.__stack = []
        self.__prepared = False
 
    def add(self, detector, parents):
        if self.__prepared:
            raise Exception("Iter Has Started: ReadOnly Now!!")

        self.__indegree[detector] = len(parents)
        for parent in parents:
            self.__topology[parent].add(detector)
 
    def __iter__(self):
        return self
 
    def next(self):
        if not self.__prepared:
            self.__stack = [d for d in self.__indegree if not self.__indegree[d]]
            self.__prepared = True

        if not self.__stack:
            raise StopIteration("DONE")

        next_detector = self.__stack.pop()
        for child in self.__topology[next_detector]:
            self.__indegree[child] -= 1
            if not self.__indegree[child]:
                self.__stack.append(child)

        return next_detector


if __name__ == '__main__':
    # FAKE_CONFIG
    test_config = {
        'A': [],
        'E': ['A'],
        'C': ['A'],
        'D': ['E', 'C'],
        'B': ['D', 'C'],
    }
    t = Topology()
    for current in test_config:
        parents = test_config[current]
        t.add(current, parents)
    for d in t:
        print d

    # TASK_CONFIG
    from config import TASK_CONFIG
    for task_name in TASK_CONFIG:
        dep = TASK_CONFIG[task_name]['DETECTORS']
        t = Topology()
        for current in dep:
            parents = dep[current]
            t.add(current, parents)
        for d in t:
            print d




