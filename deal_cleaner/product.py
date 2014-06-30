#!/usr/bin/env python
#coding=utf-8

from compdeal_config import *
from compdeal_utils import *
import logging
from collections import defaultdict
import time

def fault_tolerant(retry_counter):
    """ 持久化容错Decorator
    """
    def _fault_tolerant(function):
        def _tolerant_func(self, *args, **kwargs):
            for i in xrange(retry_counter):
                try:
                    function(self, *args, **kwargs)
                    return
                except Exception, tx:
                    logging.error("Exception Occurs While Persisting: %s" % str(tx))
                    time.sleep(10)
                    continue
            raise Exception("Persisting Failed")
        return _tolerant_func
    return _fault_tolerant

class Product:

    def __init__(self):
        self.__debug = False
        self.__clear()

    def __clear(self):
        """ 清空数据
        """
        # fact table
        self.__deals = {}           # 事实表原始值: raw data: dealid => attr_dict (from DataLoader)
        self.__deal_diff = {}       # 事实表diff值: (dealid, attr) => changed_attr
        self.__attr_diff = {}       # 事实表批量diff值: (attr, from) => to, which priority is lower than self.__deal_diff
        # dim table
        self.__dim_def = {}                     # 维度表原始值: (pk_field, pk_value) => {other_field: other_value}
        self.__dim_diff = {}                    # 维度表diff值: (pk_field, pk_value) => {changed_field: change_value}; None表示DELETE, Dict表示UPDATE
        # side effects
        self.__callbacks = []


    def reset(self, deals):
        """ 从DataLoader提取数据更新到__deals and __dim_def
        """
        # 清空原有数据
        self.__clear()

        # 事实表属性集合
        fact_table = target['table']['fact']
        fact_attrs = target['table'][fact_table]

        for dealid in deals:
            deal = deals[dealid]
            # fact_table attrs
            self.__deals[dealid] = dict((attr_name, deal[attr_name]) for attr_name in deal if attr_name in fact_attrs)
            # dim_table attrs
            for attr_name in deal:
                # 假设非事实表属性的都是维度属性
                if attr_name not in fact_attrs:
                    # 先找到维度表的主键及取值, 再将此维度表的其它属性值更新进来
                    pk_field = target['table']['pk'][attr_name]
                    pk_value = deal[pk_field]
                    if (pk_field, pk_value) not in self.__dim_def:
                        self.__dim_def[(pk_field, pk_value)] = {}
                    self.__dim_def[(pk_field, pk_value)].update({attr_name: deal[attr_name]})

    def get(self, dealid, attr):
        """ 返回指定deal的当前attr属性取值
        """
        fact_table = target['table']['fact']
        # 事实表字段
        if attr in target['table'][fact_table]:
            # 如果记录在self.__deal_diff中，直接返回
            if (dealid, attr) in self.__deal_diff:
                return self.__deal_diff[(dealid, attr)]

            # 否则先取得原值，并检查self.__attr_diff是否对其有所修改
            if dealid in self.__deals and attr in self.__deals[dealid]:
                raw_val = self.__deals[dealid][attr]
                if (attr, raw_val) in self.__attr_diff:
                    return self.__attr_diff[(attr, raw_val)]
                else:
                    return raw_val
            else:
                # WARNING: 未被初始载入self.__deals的deal返回None
                return None
        else:
            # 如果是维度表字段则先取维度表主键及取值
            pk_field = target['table']['pk'][attr]
            pk_value = self.get(dealid, pk_field)
            # 如果被修改过，那么返回更新后的取值
            if (pk_field, pk_value) in self.__dim_diff:
                update_attrs = self.__dim_diff[(pk_field, pk_value)]
                # DELETE
                if update_attrs is None:
                    return 0
                # UPDATE
                if attr in update_attrs:
                    return update_attrs[attr]
            # 未改变，返回默认初始值
            default_value = self.__dim_def[(pk_field, pk_value)][attr]
            return default_value


    def update_deal(self, dealid, attrs):
        """ Deal属性值的更新记录在deal_diff结构中
        """
        # WARNING: 未被初始载入self.__deals的deal不作处理
        if dealid not in self.__deals:
            return 0

        fact_table = target['table']['fact']
        fact_attrs = target['table'][fact_table]

        changed_attr_num = 0
        for attr, val in attrs.items():
            if attr in fact_attrs and attr in self.__deals[dealid]:
                # 仅当取值确实发现改变时才记录diff
                raw_val = self.__deals[dealid][attr]
                if raw_val != val:
                    self.__deal_diff[(dealid, attr)] = val
                    changed_attr_num += 1
                else:
                    if (dealid, attr) in self.__deal_diff:
                        del self.__deal_diff[(dealid, attr)]
                        changed_attr_num += 1

        return changed_attr_num


    def update_attr(self, attr, (from_val, to_val)):
        """ 满足attr为from_val的所有行更新为to_val，记录在attr_diff结构中
        """
        # update self.__attr_diff, with attrs constrained by target['attrs']
        fact_table = target['table']['fact']
        fact_attrs = target['table'][fact_table]

        if attr not in fact_attrs:
            return 0
        else:
            self.__attr_diff[(attr, from_val)] = to_val
            return 1


    def update_dim_def(self, pk_field, pk_value, vals = {}):
        """ 维度表属性值修改，传入维度表主键及待修改字段
        """
        # 不是维度表主键, 忽略
        if pk_field not in target['table']['dim']:
            return 0


        # 检查vals是否都是维度表的属性
        if vals is not None:
            dim_table = target['table']['dim'][pk_field]
            dim_table_fields = target['table'][dim_table]
            for k in vals:
                if k not in dim_table_fields:
                    return 0

        if (pk_field, pk_value) in self.__dim_diff:
            # 之前已有过修改
            if self.__dim_diff[(pk_field, pk_value)] is None or vals is None:
                # 如果原修改是DELETE，或者当前修改是DELETE
                self.__dim_diff[(pk_field, pk_value)] = vals
            else:
                # 否则之前和现在的修改都是UPDATE
                self.__dim_diff[(pk_field, pk_value)].update(vals)
        else:
            self.__dim_diff[(pk_field, pk_value)] = vals

        return 1

    def add_callback(self, callback, args):
        self.__callbacks.append((callback, args))

    def persist(self):
        """ 持久化生效
        """
        # Step 1: make callbacks take effect (stub)
        for callback, args in self.__callbacks:
            callback(args)

        writer = Connector.getInstance('CIS')
        try:
            cursor = writer.cursor()
            self.__persist_attr_diff(cursor)
            self.__persist_deal_diff(cursor)
            self.__persist_dim_diff(cursor)
            cursor.close()
            writer.commit()
        except Exception, tx:
            logging.error("Exception Occurs while Persisting: %s" % str(tx))
            writer.rollback()

    @fault_tolerant(3)
    def __persist_attr_diff(self, cursor):
        # Step 2: make self.__attr_diff take effects
        # UPDATE TAEGET_TABLE WHERE attr_name = from_val SET attr_name = to_val
        fact_table = target['table']['fact']
        for (attr, from_val), to_val in self.__attr_diff.items():
            converter = target['attrs'][attr][0]
            sql = "UPDATE %s SET %s = %s WHERE %s = %s" % (fact_table, attr, converter(to_val), attr, converter(from_val))
            if self.__debug:
                logging.info(sql)
            else:
                cursor.execute(sql)
            logging.info("[Product] %s: %s => %s" % (attr, from_val, to_val))


    @fault_tolerant(3)
    def __persist_deal_diff(self, cursor):
        # Step 3: make self.__deal_diff take effec by merging self.__deal_diff with self.__deals
        # UPDATE TAEGET_TABLE WHERE attr_name = from_val SET dealid = %dealid
        fact_table = target['table']['fact']
        output_deals = {}
        for dealid, attr in self.__deal_diff:
            if dealid not in output_deals:
                output_deals[dealid] = self.__deals[dealid].copy()
            if attr in output_deals[dealid]:
                converter = target['attrs'][attr][0]
                new_val = self.__deal_diff[(dealid, attr)]
                old_val = self.__deals[dealid][attr]
                output_deals[dealid][attr] = self.__deal_diff[(dealid, attr)]
                sql = "UPDATE %s SET %s = %s WHERE dealid = %d" % (fact_table, attr, converter(new_val), dealid)
                if self.__debug:
                    logging.info(sql)
                else:
                    cursor.execute(sql)
                logging.info("[Product] %d.%s: %s => %s" % (dealid, attr, old_val, new_val))

    @fault_tolerant(3)
    def __persist_dim_diff(self, cursor):
        # Step 5: Update attr_def table
        group_def_table = target['table']['dim']['groupid']
        for (attr, pk), vals in self.__dim_diff.items():
            converter = target['attrs'][attr][0]
            if vals is None:
                sql = "DELETE FROM %s WHERE %s = %s" % (group_def_table, attr, converter(pk))
            elif type(vals) == dict:
                ks = vals.keys()
                ks.append(attr)
                fields = ",".join([str(e) for e in ks])
                vs = vals.values()
                vs.append(pk)
                values = ",".join([str(e) for e in vs])
                sql = "REPLACE INTO %s(%s) VALUES (%s)" % (group_def_table, fields, values)
            else:
                raise Exception("DEF ACTION NOT SUPPORTED")
            if self.__debug:
                logging.info(sql)
            else:
                cursor.execute(sql)

    def __repr__(self):
        attr_diff = "ATTR_DIFF: %s" % str(self.__attr_diff)
        deal_diff = "DEAL_DIFF: %s" % str(self.__deal_diff)
        return attr_diff + "; " + deal_diff

