#coding=utf-8
from core.model.conf_model import ConfModel
from core.util.mysql.mysql_interface import MysqlInterface

u'''
生产环境数据库模型
'''
class ProduceDbModel(MysqlInterface):

    def __init__(self):
        systemConf =  ConfModel.getSystemConf()
        dict = {
           'host':systemConf['mysql_produce_db']['host'],
           'port':int(systemConf['mysql_produce_db']['port']),
           'user':systemConf['mysql_produce_db']['user'],
           'passwd':systemConf['mysql_produce_db']['password'],
           'db':'angejia',
           'charset':'utf8'
        }
        MysqlInterface.__init__(self,dict)


    
