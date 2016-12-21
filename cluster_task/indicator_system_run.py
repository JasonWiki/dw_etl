#coding=utf-8

u'''
dw 指标体系

调用方式
    指定日期调用: 
    ./index.py --service cluster_task --module indicator_system_run --parameter '{"date":"20151010"}'

    以当前日期为准调用,进行偏移值调用 yesterday ，today，tomorrow
    ./index.py --service cluster_task --module indicator_system_run --parameter '{"date":"today"}'
 
'''

from core.util.log.logger import Logger

from cluster_task.indicator_system import IndicatorSystem
from cluster_task.scheduler_job import SchedulerJob
from cluster_task.minireport_job import MinireportJob
from cluster_task.hive_metadata import HiveMetadata

class IndicatorSystemRun(IndicatorSystem) :


    def process(self):
        Logger.init()

        pars = self.getFormatParameter()

        u'日期'
        parsDate = pars.get('date')

        if (parsDate != None ):

            if (parsDate == "today") :
                date = self.getRegisterInstance('dateModel').getToday()
            elif (parsDate == "tomorrow") :
                date = self.getRegisterInstance('dateModel').getTomorrow()
            elif (parsDate == "yesterday") :
                date = self.getRegisterInstance('dateModel').getYesterday()
            else :
                date = parsDate
        else :
            u'默认是昨天日期 ,格式: 20151010'
            date = self.getRegisterInstance('dateModel').getYesterday()

        print date

        self.schedulerJob(date)
        self.minireportJob(date)
        self.hiveMetadata(date)
        self.dataWarehouse(date)
        self.hdfsMonitor(date)


    u' 调度指标体系任务'
    def schedulerJob(self, date):
        sc = SchedulerJob()

        # 跑存储过程
        sc.storedProcedure(date)

        # job 总数量
        jobCount = sc.getJobCount(date)
        etlJobCnStatus = self.modifyIndicatorSystem(date, 'etl_job_cn', jobCount)

        # job 总平均运行时间
        jobAvg = sc.getJobTotalAvgTime(date)
        etlJobRunAvgStatus = self.modifyIndicatorSystem(date, 'etl_job_run_avg', jobAvg)

        # 输出日志
        Logger.info("------------------------------")
        Logger.info("etl_job_cn: " + str(jobCount) + " ," + str(etlJobCnStatus) )
        Logger.info("etl_job_run_avg: " + str(jobAvg) + " ," + str(etlJobRunAvgStatus) )
        Logger.info("------------------------------")


    u' minireport 指标体系任务'
    def minireportJob(self, date):
        
        mr = MinireportJob()

        # 跑存储过程
        mr.storedProcedure(date)

        # minireprot 总数量
        minireportCount = mr.getMinireportCount(date)
        minireportCnStatus = self.modifyIndicatorSystem(date, 'minireport_cn', minireportCount)

        # minireprot总平均运行时间
        minireportAvg = mr.getMinireportTotalAvgTime(date)
        minireportRunAvgStatus = self.modifyIndicatorSystem(date, 'minireport_run_avg', minireportAvg)

        # 输出日志
        Logger.info("------------------------------")
        Logger.info("minireport_cn: " + str(minireportCount) + " ," + str(minireportCnStatus) )
        Logger.info("minireport_run_avg: " + str(minireportAvg) + " ," + str(minireportRunAvgStatus) )
        Logger.info("------------------------------")


    u' hive 元数据监控'
    def hiveMetadata(self, date):
        hm = HiveMetadata()

         # 跑存储过程
        hm.storedProcedure(date)

        # hive 数据表总条数
        hive_table_cn = hm.getHiveTableCount(date)
        hiveTableCnStatus = self.modifyIndicatorSystem(date, 'hive_table_cn', hive_table_cn)

         # 输出日志
        Logger.info("------------------------------")
        Logger.info("hive_table_cn: " + str(hive_table_cn) + " ," + str(hiveTableCnStatus) )
        Logger.info("------------------------------")


    u' 数据仓库监控'
    def dataWarehouse(self, date):
        # 获取昨天日期，因为每次统一的是当前天数昨天的日期
        offsetDay = self.getRegisterInstance('dateModel').getOffsetDateDay(date, -1)

        # dw_web_visit_traffic_log 数据表统计
        dwWebVisitTrafficLogCn = self.getTableCountForDate('dw_db.dw_web_visit_traffic_log', offsetDay);
        dwWebVisitTrafficLogCnStatus = self.modifyIndicatorSystem(date, 'dw_web_visit_traffic_log', dwWebVisitTrafficLogCn)

        # dw_web_action_detail_log 数据表统计
        dwWebActionDetailLogCn = self.getTableCountForDate('dw_db.dw_web_action_detail_log', offsetDay);
        dwWebActionDetailLogCnStatus = self.modifyIndicatorSystem(date, 'dw_web_action_detail_log', dwWebActionDetailLogCn)

        # dw_app_access_log 数据表统计
        dwAppAccessLogCn = self.getTableCountForDate('dw_db.dw_app_access_log', offsetDay);
        dwAppAccessLogCnStatus = self.modifyIndicatorSystem(date, 'dw_app_access_log', dwAppAccessLogCn)

        # dw_app_action_detail_log 数据表统计
        dwAppActionDetailLogCn = self.getTableCountForDate('dw_db.dw_app_action_detail_log', offsetDay);
        dwAppActionDetailLogCnStatus = self.modifyIndicatorSystem(date, 'dw_app_action_detail_log', dwAppActionDetailLogCn)

        # dw_property_inventory_sd 数据表统计
        dwPropertyInventorySdCn = self.getTableCountForDate('dw_db.dw_property_inventory_sd', offsetDay);
        dwPropertyInventorySdCnStatus = self.modifyIndicatorSystem(date, 'dw_property_inventory_sd', dwPropertyInventorySdCn)

         # 输出日志
        Logger.info("------------------------------")
        Logger.info(str(date) + " "  + str(offsetDay))
        Logger.info("dw_web_visit_traffic_log: " + str(dwWebVisitTrafficLogCn) + " ," + str(dwWebVisitTrafficLogCnStatus) )
        Logger.info("dw_web_action_detail_log: " + str(dwWebActionDetailLogCn) + " ," + str(dwWebActionDetailLogCnStatus) )
        Logger.info("dw_app_access_log: " + str(dwAppAccessLogCn) + " ," + str(dwAppAccessLogCnStatus) )
        Logger.info("dw_app_action_detail_log: " + str(dwAppActionDetailLogCn) + " ," + str(dwAppActionDetailLogCnStatus) )
        Logger.info("dw_property_inventory_sd: " + str(dwPropertyInventorySdCn) + " ," + str(dwPropertyInventorySdCnStatus) )
        Logger.info("------------------------------")



    u' 监控 hdfs 指标'
    def hdfsMonitor(self, date):
        rs = self.getHdfsMB()
        self.modifyIndicatorSystem(date, 'hadoop_data', rs.get('dataMb'))
        self.modifyIndicatorSystem(date, 'hadoop_hdfs', rs.get('hdfsMb'))

         # 输出日志
        Logger.info("------------------------------")
        Logger.info("hadoop_data: " + str(rs.get('dataMb')) )
        Logger.info("hadoop_hdfs: " + str(rs.get('hdfsMb')) )
        Logger.info("------------------------------")

