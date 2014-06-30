#!/usr/bin/env python
#coding=utf-8

import sys
import datetime
import time

# Extract Params
if len(sys.argv) < 2:
    print "Usage: python %s [Linkage|Mark|Type] [$date_delta]" % (__file__,)
    sys.exit()
else:
    name = sys.argv[1]
    date_delta = int(sys.argv[2]) if len(sys.argv) >= 3 else 0
    current_time = datetime.datetime.fromtimestamp(int(time.time())) - datetime.timedelta(days = date_delta)
    print name, date_delta, str(current_time)

factory_name = "Compdeal%sFactory" % name
exec "from abstract_factory import %s" % factory_name
exec "factory = %s(current_time)" % factory_name

import logging
import logging.config
logging.config.fileConfig('serverLog.config')

logging.info("Start to Invoke %s Cleaner..." % factory_name)
from director import Director
director = Director()
director.register(factory)
director.build()
logging.info("Invoke DONE")


