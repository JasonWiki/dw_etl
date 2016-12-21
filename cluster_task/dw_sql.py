#coding=utf-8

from dw_service_core import DwServiceCore
from core.util.base.file import File
from core.util.log.logger import Logger

u'''
执行 hive 和 spark sql 语句

./index.py --service cluster_task --module dw_sql --parameter '{"serverType":"spark","sql":"property/dw_property_inventory_recommend_d.sql","date":"yesterday","isDwSql":"yes"}'

'''

class DwSql(DwServiceCore) :


    def init(self):
        DwServiceCore.init(self)


    def process(self):
        # 录入参数
        parsMap = self.getFormatParameter()

        self.runDwSqlProcess(parsMap)



    u'''
    执行 Sql 流程控制
    serverType:   执行计算框架  [hive | spark]
    runEnv:       执行计算环境  [hiveserver2 | local]  本地模式
    sql:          执行的 dw_sql 仓库文件相对路径, 例如 property/dw_property_inventory_recommend_d.sql"
    isDwSql:      是否是 dw_sql 仓库的 sql [yes | no] ,默认 yes
    date:         执行的日期 [yesterday | today | yesterday | None] 执行日期, 格式 2016-11-12
    '''
    def runDwSqlProcess(self, parsMap):
        status = False

        try :
            Logger.init()

            sqlFile = parsMap.get('sql')
             # sql file 文件
            if (sqlFile == None):
                Logger.info("sql 仓库文件不存在")
                exit(1)

            # 日期
            parsDate = parsMap.get('date')

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
                # 默认是昨天日期 ,格式: 20151010
                date = self.getRegisterInstance('dateModel').getYesterday()
    
            # 服务器类型
            serverType = parsMap.get('serverType')
            if (serverType == None ):
                Logger.info("serverType : hive or spark")
                exit(1)


            # 是否是 dw 仓库的 sql 文件
            isDwSql = parsMap.get("isDwSql")
            # 读取 sql 文件内容，并且格式化好时间
            if (isDwSql == None):
                sqlContent = self.getDwSqlContent(parsMap.get('sql'), date)
            elif(isDwSql == "yes"):
               sqlContent = self.getDwSqlContent(parsMap.get('sql'), date)
            elif(isDwSql == "no"):
                sqlContent = self.getSqlContent(parsMap.get('sql'), date)
            else:
                Logger.info("isDwSql 参数: [yes|no]")
                exit(1)


            if (serverType == 'hive'):
                status = self.runSqlByHive(sqlContent, parsMap.get('runEnv'))
            elif (serverType == 'spark'):
                status = self.runSqlBySpark(sqlContent)


        except Exception,ex:
            log = "异常存储过程: "
            log += " -> " + str(Exception) + ":" + str(ex)
            Logger.info(log)

        return status


    # 执行 hive Sql
    def runSqlByHive(self, sqlContent, runEnv):
        Logger.info(sqlContent)
        Logger.info("执行中.....")

        status = False

        # 运行环境
        if (runEnv == "local"):
            u'提交到 Hive 本地 执行'
            status = self.getRegisterInstance('hiveModel').runHiveScript(sqlContent)
            u'提交到 Hive jdbc 执行'
        elif (runEnv == "hiveserver2"):
            status =  self.getRegisterInstance('hiveModel').batchExecuteSql(sqlContent)
        else :
            status =  self.getRegisterInstance('hiveModel').batchExecuteSql(sqlContent)

        u'打印日志'
        Logger.info(self.runLog("运行结果",status))
 
        return status


    # 执行 spark Sql
    def runSqlBySpark(self, sqlContent):
        Logger.info(sqlContent)
        Logger.info("执行中.....")

        u'提交到 spark jdbc 执行'
        status =  self.getRegisterInstance('sparkModel').batchExecuteSql(sqlContent)

        u'打印日志'
        Logger.info(self.runLog("运行结果",status))

        return status


    u'''
    获取 sql 文件内容内容, 并格式化 日期等参数
    file : 绝对路径
    date : 日期
    '''
    def getSqlContent(self, file, date):
        dwSqlContent = File.redeAll(file)

        formatDate = self.getRegisterInstance('dateModel').dateToFormatYmd(date)

        result = dwSqlContent.replace('${dealDate}',"'" + formatDate + "'")
        result = result.replace('${baseDealDate}',date)

        return result


    u'''
    获取 dw_sql 仓库相对路径下的文件内容
    file : dw_sql 仓库的文件的相对路径
    date : 日期
    '''
    def getDwSqlContent(self, file, date):
         # dw_sql 仓库的目录
        dwSqlDir = self.getRegisterInstance('confModel').getTaskConf()['dw_sql']['dir']

        return self.getSqlContent(dwSqlDir + "/" + file, date)



    u'''
    日志 echo 模板
    '''
    def runLog(self,log,result):

        run_log = u'''>>> start : %s

%s

result : %s

--- end

'''%(self.getRegisterInstance('dateModel').getCurrentTime(),log.decode('utf-8'),result)

        return run_log

