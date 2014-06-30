#!/usr/bin/env python
#coding=utf-8

from product import Product

##################################
# 指导器类: 掌控产品的整体构造过程
##################################
class Director:
    def __init__(self):
        self.__data_loader = None
        self.__builder = None
        self.__product = Product()

    def register(self, factory):
        """ 注册工厂

            通过给定的工厂获得DataLoader和Builder子类, 准备更新结果数据到Product
        """
        self.__data_loader = factory.get_data()
        self.__builder = factory.get_builder()

    def build(self):
        data_generator = self.__data_loader.retrieve()
        for partition_key, deal_source_attrs, deal_target_attrs in data_generator:
            self.__builder.reset(partition_key, deal_source_attrs)
            self.__product.reset(deal_target_attrs)
            self.__builder.build(self.__product)
            self.__product.persist()





