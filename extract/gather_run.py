#coding=utf-8

u'''
聚合 run
'''
from dw_service_core import DwServiceCore

from extract.gather_table import GatherTable
from core.util.log.logger import Logger

u'''
聚合快照表到一张 hive 分区表中

调用方法： 
  1. gather 所有表
     ./index.py --service extract --module gather_run --parameter '{}'

  2. 聚合指定表
    ./index.py --service extract --module gather_run --parameter '{"date":"2016-01-28","dbName":"angejia","tbName":"inventory_detail_survey"}'   

    date 分区日期,默认昨天,格式  yyyy-mm-dd
    dbName 数据库名
    tbName 表名
        
'''
class GatherRun(DwServiceCore) :

    def process(self):
        Logger.init()

        # 解析参数
        parameter = self.getFormatParameter()

        dbName =  parameter.get('dbName')
        tbName =  parameter.get('tbName')
        date =  parameter.get('date')

        if (date == None):
            date = self.getRegisterInstance('dateModel').getYesterdayByYmd()

        # gather 指定数据表
        if (dbName != None and tbName != None):
            self.gatherTable(dbName,tbName,date)
        # gather 所有表
        else:
            self.gatherTableAll(date)

        #print dbServer , dbName , tbName , date


    # 聚合数据表
    def gatherTable(self,dbName,tbName,date):
        try :
            extractConf = self.getRegisterInstance('confModel').getExtractConf()
            
            confSeparator = extractConf['core']['separator']
            confSourceDb = extractConf['gather_table']['hive_source_db']
            confTargetDb = extractConf['gather_table']['hive_target_db']
    
            # 设置 gather 表规则
            sourceDb = confSourceDb
            sourceTable = dbName + confSeparator + tbName
            targetDb = confTargetDb
            targetTable = dbName + confSeparator + tbName
    
            gatherTable = GatherTable()
            gatherTable.setSourceDb(sourceDb)
            gatherTable.setSourceTable(sourceTable)
            gatherTable.setTargetDb(targetDb)
            gatherTable.setTargetTable(targetTable)
            gatherTable.setPartitionDate(date)
            gatherTable.run()

        except Exception,ex:
            log = "异常->  " + str(sourceDb) + "." + str(sourceTable) 
            log += " -> " + str(Exception) + ":" + str(ex)
            Logger.info(log)

    # 聚合所有数据表
    def gatherTableAll (self,date):
        # 获取抽取列表
        gatherTables = self.getRegisterInstance('biDbModel').getGatherTables()

        for curTableInfo in gatherTables:
            # 源数据信息
            dbName = curTableInfo['db_name']
            tbName = curTableInfo['tb_name']

            self.gatherTable(dbName, tbName, date)



    # 聚合 table 
    def gatherTableBak(self,date):
        extractConf = self.getRegisterInstance('confModel').getExtractConf()
        
        confSeparator = extractConf['core']['separator']
        confSourceDb = extractConf['gather_table']['hive_source_db']
        confTargetDb = extractConf['gather_table']['hive_target_db']

        # 获取抽取列表
        gatherTables = self.getRegisterInstance('biDbModel').getGatherTables()
        for curTableInfo in gatherTables:
            # 源数据信息
            dbServer = curTableInfo['db_server']
            dbName = curTableInfo['db_name']
            tbName = curTableInfo['tb_name']

            # 设置 gather 表规则
            sourceDb = confSourceDb
            sourceTable = dbName + confSeparator + tbName
            targetDb = confTargetDb
            targetTable = dbName + confSeparator + tbName

            gatherTable = GatherTable()
            gatherTable.setSourceDb(sourceDb)
            gatherTable.setSourceTable(sourceTable)
            gatherTable.setTargetDb(targetDb)
            gatherTable.setTargetTable(targetTable)
            gatherTable.setPartitionDate(date)
            gatherTable.run()

