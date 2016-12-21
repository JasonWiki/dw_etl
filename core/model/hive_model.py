#coding=utf-8
from core.model.conf_model import ConfModel
from core.util.hive.hive_interface import HiveInterface


class HiveModel(HiveInterface):

    def __init__(self):
        systemConf =  ConfModel.getSystemConf()
        dict = {
           'host':systemConf['hive_server2']['host'],
           'port':systemConf['hive_server2']['port'],
           'user':systemConf['hive_server2']['user'],
           'password':systemConf['hive_server2']['password'],
           'hiveHome' : systemConf['hive_server2']['hive_home']
           
        }
        HiveInterface.__init__(self,dict)
