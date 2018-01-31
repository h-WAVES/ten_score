#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@version: 1.0
@author: hht
@software: PyCharm Community Edition
@file: conn_mongodb.py
@time: 17-11-28下午4:57
"""
from gaode_hotel.conn_mongodb import conn_mongodb
from pymongo import MongoClient
class split_mfw(object):
    def get_mafengwo(self):
        Client=MongoClient('192.168.1.105',27017)
        db_first=Client.gaode_pois
        db_2 = conn_mongodb("gaode_pois","three_province_poi_v11")
        #  #删除2中的马蜂窝数据
        # db_2.delete({"site":"mafengwo"})
        # db_mfw_spilt_test=conn_mongodb("merge_data_mid","mfw_spilt_test")
        # data=db.db_find({})
        #根据数量插入新的数据（1-10）
        count=db_first.three_province_poi_v9.find({"sum_n.sum_4_x":{ "$gt":0} } ).sort([("sum_n.sum_4_x",1)]).count()
        print(count)   #14749
        a=int(count/10)
        for i in range(10):
            skip_num=i*a
            if i==9:
                data=db_first.three_province_poi_v9.find({"sum_n.sum_4_x":{ "$gt":0} } ).sort([("sum_n.sum_4_x",1)]).skip(skip_num).limit(a+9)
            else:
                data=db_first.three_province_poi_v9.find({"sum_n.sum_4_x":{ "$gt":0} } ).sort([("sum_n.sum_4_x",1)]).skip(skip_num).limit(a)

            list=[]
            for x in data:
                print(x["sum_n"]["sum_4_x"])
                x["food_rating"]=i+1
                print(x["food_rating"])
                #
                db_2.insert_data_db(x)

        #插入原来是0分的数据
        data_0=db_first.three_province_poi_v9.find({ "sum_n.sum_4_x":0} )
        for y in data_0:
             print(y["sum_n"]["sum_4_x"])
             y["food_rating"]=0
             print(y["food_rating"])
             db_2.insert_data_db(y)



start=split_mfw()
start.get_mafengwo()
