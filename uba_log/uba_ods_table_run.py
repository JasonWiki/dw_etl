#coding=utf-8
import time
import datetime

from dw_service_core import DwServiceCore
from cluster_task.dw_sql import DwSql
from core.util.log.logger import Logger

u'''
uba ods table log 处理
调用方法： 
    单独跑一张 dw_web_action_detail_log 
    ./index.py --service uba_log --module uba_ods_table_run --parameter '{"serverType":"spark","date":"yesterday","logType":"dw_app_access_log","isDwSql":"no"}'
    ./index.py --service uba_log --module uba_ods_table_run --parameter '{"serverType":"spark","date":"yesterday","logType":"dw_app_action_detail_log","isDwSql":"no"}'
    ./index.py --service uba_log --module uba_ods_table_run --parameter '{"serverType":"spark","date":"yesterday","logType":"dw_web_visit_traffic_log","isDwSql":"no"}'
    ./index.py --service uba_log --module uba_ods_table_run --parameter '{"serverType":"spark","date":"yesterday","logType":"dw_web_action_detail_log","isDwSql":"no"}'

    OR 

    # 跑所有的 log 
    ./index.py --service uba_log --module uba_ods_table_run --parameter '{"serverType":"spark","date":"yesterday","logType":"","isDwSql":"no"}'
'''

class UbaOdsTableRun(DwServiceCore) :

    dwSqlServcie = None

    # uba sql 所在目录
    ubaSqlPath = None

    # 最终状态:
    accessLogStatus = None
    dwAccessLogStatus = None
    ubaAppActionLogStatus = None
    ubaWebVisitLogStatus = None
    ubaWebActionLogStatus = None


    def init(self):
        DwServiceCore.init(self)

        Logger.init()

        self.ubaSqlPath = self.getDwCoreInstance().SystemPath('basePath') + '/uba_log/uba_sql'


    def process(self):
        # 录入参数:
        parsMap = self.getFormatParameter()

        self.createOdsTable(parsMap)

    
    def createOdsTable(self, parsMap):
        # 执行控制
        self.dwSqlServcie = DwSql()

        # 日志类型
        logType = parsMap.get('logType')
        if (logType == None or logType == '') :
            self.accessLogStatus = self.accessLog(parsMap)
            self.dwAccessLogStatus = self.dwAccessLog(parsMap)
            self.ubaAppActionLogStatus = self.ubaAppActionLog(parsMap)
            self.ubaWebVisitLogStatus = self.ubaWebVisitLog(parsMap)
            self.ubaWebActionLogStatus = self.ubaWebActionLog(parsMap)

        elif (logType == 'access_log' ):
            self.accessLogStatus = self.accessLog(parsMap)

        elif (logType == 'dw_access_log' ):
            self.dwAccessLogStatus = self.dwAccessLog(parsMap)

        elif (logType == 'uba_app_action_log' ):
            self.ubaAppActionLogStatus = self.ubaAppActionLog(parsMap)

        elif (logType == 'uba_web_visit_log' ):
            self.ubaWebVisitLogStatus = self.ubaWebVisitLog(parsMap)

        elif (logType == 'uba_web_action_log' ):
            self.ubaWebActionLogStatus = self.ubaWebActionLog(parsMap)


    def accessLog(self, parsMap):
        parsData = parsMap
        parsData.update( {"sql" : self.ubaSqlPath + "/ods/access_log.sql"} )
        return self.dwSqlServcie.runDwSqlProcess(parsData)


    def dwAccessLog(self, parsMap):
        parsData = parsMap
        parsData.update( {"sql" : self.ubaSqlPath + "/ods/dw_access_log.sql"} )
        return self.dwSqlServcie.runDwSqlProcess(parsData)


    def ubaAppActionLog(self, parsMap):
        parsData = parsMap
        parsData.update( {"sql" : self.ubaSqlPath + "/ods/uba_app_action_log.sql"} )
        return self.dwSqlServcie.runDwSqlProcess(parsData)


    def ubaWebVisitLog(self, parsMap):
        parsData = parsMap
        parsData.update( {"sql" : self.ubaSqlPath + "/ods/uba_web_visit_log.sql"} )
        return self.dwSqlServcie.runDwSqlProcess(parsData)


    def ubaWebActionLog(self, parsMap):
        parsData = parsMap
        parsData.update( {"sql" : self.ubaSqlPath + "/ods/uba_web_action_log.sql"} )
        return self.dwSqlServcie.runDwSqlProcess(parsData)


    def shutdown(self):
        Logger.info("执行结果 access_log : " + str(self.accessLogStatus))
        Logger.info("执行结果 dw_access_log : " + str(self.dwAccessLogStatus))
        Logger.info("执行结果 uba_app_action_log : " + str(self.ubaAppActionLogStatus))
        Logger.info("执行结果 uba_web_visit_log : " + str(self.ubaWebVisitLogStatus))
        Logger.info("执行结果 uba_web_action_log : " + str(self.ubaWebActionLogStatus))


    
















