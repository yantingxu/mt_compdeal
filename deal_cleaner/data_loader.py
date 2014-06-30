#!/usr/bin/env python
#coding=utf-8

import MySQLdb
import re
import sys
import logging
import time
from compdeal_utils import Connector
from compdeal_config import db_meta, source, target

###########################
# 中间产品类: 数据载入器
###########################
class BaseDataLoader(object):

    def __init__(self, current_time):
        self.__current_time = current_time

    def get_current_time(self):
        return self.__current_time

    def _get_partition_key_sql(self):
        raise Exception("PK SQL MUST BE PROVIDED!!")

    def _generate_partition_keys(self):
        sql = self._get_partition_key_sql()
        reader = Connector.getInstance()
        cursor = reader.cursor(cursorclass = MySQLdb.cursors.DictCursor)
        cursor.execute(sql)
        #logging.info(sql)

        field_name = converter = None
        for line in cursor:
            if field_name is None:
                field_name = line.keys()[0]
                converter = source['attrs'][field_name][0] if field_name in source['attrs'] else target['attrs'][field_name][0]
            yield converter(line[field_name])
        cursor.close()

    def _get_deal_sql(self):
        # template with partition_key to be replaced
        raise Exception("DEAL SQL MUST BE PROVIDED!!")

    def retrieve(self):
        current_time = self.get_current_time()
        logging.info("Generating Partition keys...")
        partition_keys = self._generate_partition_keys()
        logging.info("DONE.")

        sql_template = self._get_deal_sql()
        reader = Connector.getInstance()
        cursor = reader.cursor(cursorclass = MySQLdb.cursors.DictCursor)
        pk_count = 0
        for partition_key in partition_keys:
            deal_source_attrs = {}
            deal_target_attrs = {}
            sql = sql_template % {'partition_key': partition_key, 'current_time': current_time}
            #logging.info(sql)
            #logging.info("Excuting SQL...")
            cursor.execute(sql)

            #logging.info("Parsing...")
            for line in cursor:
                dealid = int(line['dealid'])
                deal_attrs = self._parse(dealid, line)
                deal_source_attrs[dealid] = deal_attrs[0]
                deal_target_attrs[dealid] = deal_attrs[1]

            #logging.info("DONE")
            pk_count += 1
            if pk_count % 10000 == 0:
                logging.info("PK_COUNT: %d" % pk_count)
            yield partition_key, deal_source_attrs, deal_target_attrs

    def _parse(self, dealid, line):
        """ 解析数据，对deal的源属性和目标属性分组
        """
        source_attrs = {'dealid': dealid}
        target_attrs = {'dealid': dealid}
        for field in line:
            if field in target['attrs']:
                converter = target['attrs'][field][0]
                val = converter(line[field])
                target_attrs[field] = val
            elif field in source['attrs']:
                converter = source['attrs'][field][0]
                val = converter(line[field])
                source_attrs[field] = val
            else:
                pass
        return source_attrs, target_attrs


class LinkageLoader(BaseDataLoader):

    def __init__(self, current_time):
        super(LinkageLoader, self).__init__(current_time)

    def _get_partition_key_sql(self):
        sql = """
        """
        current_time = self.get_current_time()
        sql = sql % {'current_time': current_time}
        #logging.info(sql)
        return sql


    def _get_deal_sql(self):
        sql = """
        """
        return sql



class MarkLoader(BaseDataLoader):

    def __init__(self, current_time):
        super(MarkLoader, self).__init__(current_time)

    def _get_partition_key_sql(self):
        current_time = self.get_current_time()
        group_sql = """
        """ % {'current_time': current_time}
        return group_sql

    def _get_deal_sql(self):
        attr_sql = """
        """
        return attr_sql



class TypeLoader(BaseDataLoader):

    def __init__(self, current_time):
        super(TypeLoader, self).__init__(current_time)


    def _get_partition_key_sql(self):
        sql = """
        """
        current_time = self.get_current_time()
        sql = sql % {'current_time': current_time}
        return sql

    def _get_deal_sql(self):
        sql = """
        """

        return sql
