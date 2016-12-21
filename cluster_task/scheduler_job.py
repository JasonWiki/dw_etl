#coding=utf-8

from dw_service_core import DwServiceCore
from cluster_task.dw_sql import DwSql

u'''
SchedulerJob 监控
'''

class SchedulerJob(DwServiceCore) :


    # 执行存储过程
    def storedProcedure(self, date):
        # 存储构成的 sql 文件
        sqlFile = 'monitor/monitor_scheduler_job.sql'

        sqlContent = DwSql().getDwSqlContent(sqlFile, date)

        return self.getRegisterInstance('biDbModel').batchExecuteSql(sqlContent)


    # 获取 Job 数量
    def getJobCount(self, date):
        formatDate = self.getRegisterInstance('dateModel').dateToFormatYmd(date)
        querySql = "SELECT COUNT(*) AS c FROM dw_service.scheduler_job_sd WHERE p_dt='"+formatDate+"';"
        return self.getRegisterInstance('biDbModel').count(querySql)


    # 获取所有 JOB 总的运行时间
    def getJobTotalRunTime(self, date):
        formatDate = self.getRegisterInstance('dateModel').dateToFormatYmd(date)
        querySql = "SELECT SUM(last_second) AS c FROM dw_service.scheduler_job_sd WHERE p_dt='"+formatDate+"';"
        return self.getRegisterInstance('biDbModel').count(querySql)


    # 获取所有 JOB 平均运行时间
    def getJobTotalAvgTime(self,date):
         cn = self.getJobTotalRunTime(date) / self.getJobCount(date)
         return int(cn)
