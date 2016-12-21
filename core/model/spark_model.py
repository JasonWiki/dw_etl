#coding=utf-8
u'''
spark-sql 模型

底层通讯，基于 HiveServer2 的实现
'''

from core.model.conf_model import ConfModel
from core.util.hive.hive_server2 import HiveServer2
from core.util.hive.hive_interface import HiveInterface


class SparkModel(HiveInterface):

    def __init__(self):
        systemConf =  ConfModel.getSystemConf()
        dict = {
           'host':systemConf['spark_server']['host'],
           'port':systemConf['spark_server']['port'],
           'user':systemConf['spark_server']['user'],
           'password':systemConf['spark_server']['password']
        }
        HiveInterface.__init__(self,dict)



class SparkModelBak:

    sparkInterface = None
    def _getSparkInterface(self):
        systemConf =  ConfModel.getSystemConf()

        u'避免多次创建连接实例'
        if (self.sparkInterface != None):
            return self.sparkInterface

        dict = {
           'host':systemConf['spark_server']['host'],
           'port':systemConf['spark_server']['port'],
           'user':systemConf['spark_server']['user'],
           'password':systemConf['spark_server']['password'],
        }

        self.sparkInterface = HiveServer2(dict)
        return self.sparkInterface


    u'''
    创建数据表
    返回 bool 值
    '''
    def createTable(self,createTableSql):
        return self._getSparkInterface().execute([createTableSql])

    u'''
    删除表
    返回 bool 值
    '''
    def dropTable(self,dbTbName):
        dropTableSql = "DROP TABLE IF EXISTS " + dbTbName;
        return self._getSparkInterface().execute([dropTableSql])


    u'''
    执行 spark-sql 语句
    '''
    def runSparkSqlBak(self,sqlContent):
        sqlContentList = sqlContent.split(';');
        formatSqlContentList = sqlContentList[:-1];

        return self._getSparkInterface().execute(formatSqlContentList)

    