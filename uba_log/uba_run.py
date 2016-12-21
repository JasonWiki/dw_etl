#coding=utf-8
import time
import datetime

from dw_service_core import DwServiceCore
from cluster_task.dw_sql import DwSql
from core.util.log.logger import Logger

u'''
uba log 处理
调用方法： 
    单独跑一张 dw_web_action_detail_log 
    ./index.py --service uba_log --module uba_run --parameter '{"serverType":"spark","date":"yesterday","logType":"dw_app_access_log","isDwSql":"no"}'
    ./index.py --service uba_log --module uba_run --parameter '{"serverType":"spark","date":"yesterday","logType":"dw_app_action_detail_log","isDwSql":"no"}'
    ./index.py --service uba_log --module uba_run --parameter '{"serverType":"spark","date":"yesterday","logType":"dw_web_visit_traffic_log","isDwSql":"no"}'
    ./index.py --service uba_log --module uba_run --parameter '{"serverType":"spark","date":"yesterday","logType":"dw_web_action_detail_log","isDwSql":"no"}'

    OR 

    # 跑所有的 log 
    ./index.py --service uba_log --module uba_run --parameter '{"serverType":"spark","date":"yesterday","logType":"","isDwSql":"no"}'
'''

class UbaRun(DwServiceCore) :

    dwSqlServcie = None

    # uba sql 所在目录
    ubaSqlPath = None

    # 最终状态:
    dwAppAccessLogStatus = None
    dwAppActionDetailLogStatus = None
    dwWebVisitTrafficLogStatus = None
    dwWebActionDetailLogStatus = None


    def init(self):
        DwServiceCore.init(self)

        self.ubaSqlPath = self.getDwCoreInstance().SystemPath('basePath') + '/uba_log/uba_sql'


    def process(self):
        Logger.init()
        # 录入参数:
        parsMap = self.getFormatParameter()

        # 执行控制
        self.dwSqlServcie = DwSql()

        # 日志类型
        logType = parsMap.get('logType')
        if (logType == None or logType == '') :
             # APP 主题  LOG
            self.dwAppAccessLogStatus = self.dwAppAccessLog(parsMap)
            self.dwAppActionDetailLogStatus = self.dwAppActionDetailLog(parsMap)
    
            # WEB 主题 Log
            self.dwWebVisitTrafficLogStatus = self.dwWebVisitTrafficLog(parsMap)
            self.dwWebActionDetailLogStatus = self.dwWebActionDetailLog(parsMap)

        elif (logType == 'dw_app_access_log' ):
            self.dwAppAccessLogStatus = self.dwAppAccessLog(parsMap)

        elif (logType == 'dw_app_action_detail_log' ):
            self.dwAppActionDetailLogStatus = self.dwAppActionDetailLog(parsMap)

        elif (logType == 'dw_web_visit_traffic_log' ):
            self.dwWebVisitTrafficLogStatus = sself.dwWebVisitTrafficLog(parsMap)

        elif (logType == 'dw_web_action_detail_log' ):
            self.dwWebActionDetailLogStatus =  self.dwWebActionDetailLog(parsMap)


    # app 访问 access_log
    def dwAppAccessLog(self, parsMap):
        parsData = parsMap
        parsData.update( {"sql" : self.ubaSqlPath + "/app/dw_app_access_log.sql"} )
        return self.dwSqlServcie.runDwSqlProcess(parsData)


    # app 用户行为log
    def dwAppActionDetailLog(self, parsMap):
        parsData = parsMap
        parsData.update( {"sql" : self.ubaSqlPath + "/app/dw_app_action_detail_log.sql"} )
        return self.dwSqlServcie.runDwSqlProcess(parsData)


    # web 访问 log
    def dwWebVisitTrafficLog(self, parsMap):
        parsData = parsMap
        parsData.update( {"sql" : self.ubaSqlPath + "/web/dw_web_visit_traffic_log.sql"} )
        return self.dwSqlServcie.runDwSqlProcess(parsData)


    # web 行为 log
    def dwWebActionDetailLog(self, parsMap):
        parsData = parsMap
        parsData.update( {"sql" : self.ubaSqlPath + "/web/dw_web_action_detail_log.sql"} )
        return self.dwSqlServcie.runDwSqlProcess(parsData)


    def shutdown(self):
        Logger.info("执行结果 dw_app_access_log : " + str(self.dwAppAccessLogStatus))
        Logger.info("执行结果 dw_app_action_detail_log : " + str(self.dwAppActionDetailLogStatus))
        Logger.info("执行结果 dw_web_visit_traffic_log : " + str(self.dwWebVisitTrafficLogStatus))
        Logger.info("执行结果 dw_web_action_detail_log : " + str(self.dwWebActionDetailLogStatus))


    
















