#coding=utf-8

from dw_service_core import DwServiceCore
from cluster_task.dw_sql import DwSql

u'''
MinireportJob 监控
'''

class MinireportJob(DwServiceCore) :

    # 执行存储过程
    def storedProcedure(self, date):
        # 存储构成的 sql 文件
        sqlFile = 'monitor/monitor_minireport.sql'

        sqlContent = DwSql().getDwSqlContent(sqlFile, date)

        return self.getRegisterInstance('biDbModel').batchExecuteSql(sqlContent)


    # 获取 Minireport 数量
    def getMinireportCount(self, date):
        formatDate = self.getRegisterInstance('dateModel').dateToFormatYmd(date)
        querySql = "SELECT COUNT(*) AS c FROM dw_service.minireport_job_sd WHERE p_dt='"+formatDate+"';"
        return self.getRegisterInstance('biDbModel').count(querySql)


    # 获取所有 Minireport 总的运行时间
    def getMinireportTotalRunTime(self, date):
        formatDate = self.getRegisterInstance('dateModel').dateToFormatYmd(date)
        querySql = "SELECT SUM(last_second) AS c FROM dw_service.minireport_job_sd WHERE p_dt='"+formatDate+"';"
        return self.getRegisterInstance('biDbModel').count(querySql)


    # 获取所有 Minireport 平均运行时间
    def getMinireportTotalAvgTime(self,date):
         cn = self.getMinireportTotalRunTime(date) / self.getMinireportCount(date)
         return int(cn)
