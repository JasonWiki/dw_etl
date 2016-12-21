#coding=utf-8

from dw_service_core import DwServiceCore
from cluster_task.dw_sql import DwSql

u'''
hive 元数据监控
'''
class HiveMetadata(DwServiceCore) :

    def __init__(self):
        DwServiceCore.init(self)
        self.setRegisterInstance('hiveDbModel', self.getDwCoreInstance().getModelInterface('HiveDb') )

    # 执行存储过程
    def storedProcedure(self, date):
        # 存储构成的 sql 文件
        sqlFile = 'monitor/monitor_hive_table.sql'

        sqlContent = DwSql().getDwSqlContent(sqlFile, date)

        return self.getRegisterInstance('hiveDbModel').batchExecuteSql(sqlContent)


    # 获取 hive 总条数
    def getHiveTableCount(self, date):
        formatDate = self.getRegisterInstance('dateModel').dateToFormatYmd(date)
        querySql = "SELECT COUNT(*) AS c FROM test.hive_table_history WHERE p_dt='"+formatDate+"';"
        return self.getRegisterInstance('hiveDbModel').count(querySql)

