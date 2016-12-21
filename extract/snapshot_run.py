#coding=utf-8

u'''
镜像run
'''
from dw_service_core import DwServiceCore
from core.util.log.logger import Logger

u'''
镜像拷贝一张表到hive带时间戳

调用方法： 
  1. snapshot所有表
     ./index.py --service extract --module snapshot_run --parameter '{}'

  2. snapshot指定表
    ./index.py --service extract --module snapshot_run --parameter '{"date":"2016-12-01","dbName":"xinfang","tbName":"loupan_basic"}'   

    date 生成镜像日期,默认昨天,格式  yyyy-mm-dd
    dbName 数据库名
    tbName 表名
        
'''
class SnapshotRun(DwServiceCore) :

    def process(self):
        Logger.init()

        # 解析参数
        parameter = self.getFormatParameter()

        dbName = parameter.get('dbName')
        tbName = parameter.get('tbName')
        date = parameter.get('date')

        if (date == None):
            date = self.getRegisterInstance('dateModel').getYesterdayByYmd()
        # snapshot 指定数据表
        if (dbName != None and tbName != None):
            self.snapshotTable(dbName,tbName,date)
        # snapshot 所有表
        else:
            self.snapshotTableAll(date)
        
    #snapshot指定表    
    def snapshotTable(self,dbName,tbName,date):
        try :
            extractConf = self.getRegisterInstance('confModel').getExtractConf()
            
            confSeparator = extractConf['core']['separator']
            confSourceDb = extractConf['snapshot_table']['hive_source_db']
            confTargetDb = extractConf['snapshot_table']['hive_target_db']
    
            # 设置 snapshot表规则
            sourceDb = confSourceDb
            sourceTable = dbName + confSeparator + tbName
            targetDb = confTargetDb
            targetTable = dbName + confSeparator + tbName + '_' + date.replace('-','')
    
            # 开始时间
            startTimestamp = self.getRegisterInstance('dateModel').getTimestamp()

            # 1.生成sql
            createHiveTableSql = '''
DROP TABLE IF EXISTS %(snapshotTbl)s; 
CREATE TABLE IF NOT EXISTS %(snapshotTbl)s AS
SELECT * FROM %(srcTbl)s;'''%{'srcTbl':sourceDb + '.' + sourceTable,
                             'snapshotTbl': targetDb + '.' + targetTable}
            
            Result = self.getRegisterInstance('hiveModel').batchExecuteSql(createHiveTableSql)
            # 2.计算执行结果写日志和打印
            diffTimestamp = self.getRegisterInstance('dateModel').getTimestamp() - startTimestamp
            logSql = "INSERT INTO dw_service.snapshot_log (`source_db`,`source_table`,`target_db`,`target_table`,`code`,`run_time`) "
            logSql += "VALUES ('%s','%s','%s','%s',%d,%d)"
            logSql = logSql%(sourceDb,sourceTable,targetDb,targetTable,int(Result),diffTimestamp)
            self.getRegisterInstance('biDbModel').insertData(logSql)
 
            # 打印日志
            logStr = str(sourceDb) + "." + str(sourceTable) + " -> " + str(targetDb) +  "." + str(targetTable) + " Time : " + str(diffTimestamp) + ")"
            Logger.info(logStr)
             
        except Exception,ex:
            log = "异常->  " + str(sourceDb) + "." + str(sourceTable) 
            log += " -> " + str(Exception) + ":" + str(ex)
            Logger.info(log)
    
    #snapshot所有表
    def snapshotTableAll(self,date):
        # 获取抽取列表
        snapshotTables = self.getRegisterInstance('biDbModel').getGatherTables()
        for curTableInfo in snapshotTables:
            # 源数据信息
            dbName = curTableInfo['db_name']
            tbName = curTableInfo['tb_name']
            self.snapshotTable(dbName, tbName, date)
