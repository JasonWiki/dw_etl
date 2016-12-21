#coding=utf-8
from core.model.conf_model import ConfModel
from core.util.mysql.mysql_interface import MysqlInterface

u'''
hive 元数据库
'''
class HiveDbModel(MysqlInterface):

    def __init__(self):
        systemConf =  ConfModel.getSystemConf()
        dict = {
           'host':systemConf['mysql_hive_db']['host'],
           'port':int(systemConf['mysql_hive_db']['port']),
           'user':systemConf['mysql_hive_db']['user'],
           'passwd':systemConf['mysql_hive_db']['password'],
           'db':'hive',
           'charset':'utf8'
        }
        MysqlInterface.__init__(self,dict)


    
