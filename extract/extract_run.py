#coding=utf-8
u'''
数据抽取执行入口
'''
import threading
from time import ctime,sleep
import Queue

from dw_service_core import DwServiceCore
from extract.extract_mysql import ExtractMysql
from core.util.log.logger import Logger


u'''
抽取 mysql 数据到 hive 中

调用方法： 
    1. 抽取 串行/并行
        runType : liste 串行抽取, thread 并行抽取
    ./index.py --service extract --module extract_run --parameter '{"runType":"liste"}'
    
    ./index.py --service extract --module extract_run --parameter '{"runType":"thread"}'

    2. 抽取单个数据表
        dbServer 抽取服务器: product 业务数据库 ,dw 数据部服务器
        sourceDb 源数据库 
        sourceTb 源数据表
        targetDb 目标数据库
        targetTb 目标数据表
        extractTool 抽取工具: 1 mysql dump , 2 sqoop
        extractType 抽取类型: 1 全量, 2 增量
        mapReduceNum  mapReduce 数量 ,抽取类型为 2 时有效
../index.py \
--service extract \
--module extract_run \
--par '{"dbServer":"product","sourceDb":"angejia","sourceTb":"user_msg","targetDb":"db_sync","targetTb":"angejia__user_msg","extractType":"1","extractTool":"1","mapReduceNum":"1"}'   

'''
class ExtractRun(DwServiceCore) :

    def process(self):
        Logger.init()

         # 解析参数
        parameter = self.getFormatParameter()

        # 运行类型
        runType = parameter.get('runType')

        # 串行抽取
        if (runType == 'liste'):
            self.extractMysqlTableListe(ExtractMysql.COMPLETE)
            self.extractMysqlTableListe(ExtractMysql.INCREMENTAL)
         # 并行抽取
        elif (runType == 'thread'):
            self.extractMysqlTableThread()
            self.extractMysqlTableListe(ExtractMysql.INCREMENTAL)
        # 指定抽取数据表抽取
        elif (parameter.get('sourceDb') != None and parameter.get('sourceTb') != None):
            self.extractMysqlTableIndependent(parameter)
        # 测试
        else :
            self.extractMysqlTableTest()


    # 并发执行
    def extractMysqlTableThread(self):
  
        # 等待运行数据表
        sourceTableList = self.getRegisterInstance('biDbModel').getExtractMysqlTables(ExtractMysql.COMPLETE)
        #sourceTableList = self.getRegisterInstance('biDbModel').getExtractMysqlTables(ExtractMysql.INCREMENTAL)

        # 运行线程数
        numThreadPool = 4

        # 定义队列
        q = Queue.Queue()

        # 加入到队列中
        for curTableInfo in sourceTableList:
             q.put(curTableInfo)


        # 开指定数量线程消费
        for curThreadPoolNum in range(numThreadPool):
            currentThread = threading.Thread(target=self.runTable,args=(q, curThreadPoolNum))
            # True：父进程不等待子进程结束, 继续执行
            # False：父进程会等待所有子进程执行完毕,父进程才会退出
            currentThread.setDaemon(True)
            currentThread.start()

        # 等到队列为空，再向下执行
        q.join()

        Logger.info('执行完成~')

        return True


    def runTable(self, q, threadPoolNum):

        wNum = 1
        while(True):
            # 队列为空的时候退出
            if (q.empty() == True):
                break

            # 当前可消费的队列
            qTableInfo = q.get()

            sourceTb = str(qTableInfo['id']) + ': ' + qTableInfo['db_name'] + '.' + qTableInfo['tb_name']
            Logger.info('线程:' + str(threadPoolNum) + ', 第: ' + str(wNum) + ' 次-> ' + str(sourceTb))

            # 执行抽取任务
            self.extractMysqlTable(qTableInfo)

            #hiveTbName =  "test." + qTableInfo['db_name'] + "__" + qTableInfo['tb_name']
            #print hiveTbName
            #print self.hiveModel.dropTable(hiveTbName)

            q.task_done()
            wNum += 1


    # 串行 抽取 MYSQL 数据表 
    def extractMysqlTableListe(self, extractType):
         extractMysqlTables = self.getRegisterInstance("biDbModel").getExtractMysqlTables(extractType)

         for curTableInfo in extractMysqlTables:
             self.extractMysqlTable(curTableInfo)



    # 抽取方法实体
    def extractMysqlTable(self, tableInfo):
        try :
            extractConf = self.getRegisterInstance('confModel').getExtractConf()

            # 默认表分隔符
            confSeparator = extractConf['core']['separator']
            # 抽取到目标 hive 数据库名
            confTargetDb = extractConf['extract_mysql']['hive_target_db']
            # dump 本地临时目录
            confDumpFileDir = extractConf['extract_mysql']['dump_file_dir']

            # 数据库配置表信息

            # tb id
            tbId = tableInfo['id']
            # mysql 数据库源信息
            dbServer = tableInfo['db_server']
            # 数据源: 数据库名
            dbName = tableInfo['db_name']
            # 数据源: 表名
            tbName = tableInfo['tb_name']
            # 目标 hive 数据库名
            dbTargetDbName = tableInfo['target_db_name']
            # 目标 hive 表名
            dbTargetTbName = tableInfo['target_tb_name']
            # 抽取工具
            extractTool = tableInfo['extract_tool']
            # 抽取类型
            extractType = tableInfo['extract_type']

            # 设置 hive 表名的规则
            # 当指定了抽取的目标库, 使用指定的库和表名
            if (dbTargetDbName != "" and dbTargetTbName !=""):
                affirmTargetDb = dbTargetDbName
                affirmTargetTb = dbTargetTbName
            # 没有使用则用默认的规则
            else:
                affirmTargetDb = confTargetDb
                affirmTargetTb = dbName + confSeparator + tbName

            # 实例化抽取对象
            extractMysql = ExtractMysql()
            # Dump 方式时的保存目录
            extractMysql.setDumpFileDir(confDumpFileDir)
    
            # 设置抽取的数据库源
            if (dbServer == ExtractMysql.PRODUCE_DB):
                  extractMysql.setExtractDb(ExtractMysql.PRODUCE_DB)
            elif (dbServer == ExtractMysql.DW_DB):
                 extractMysql.setExtractDb(ExtractMysql.DW_DB)
            else:
                 Logger.info("抽取的数据源不存在！" + dbServer)


            # 设置抽取类型
            # 全量抽取
            if (extractType == ExtractMysql.COMPLETE):
                extractMysql.setExtractType(ExtractMysql.COMPLETE)
            # 增量抽取
            elif (extractType == ExtractMysql.INCREMENTAL):
                extractMysql.setExtractType(ExtractMysql.INCREMENTAL)
            else :
                Logger.info("抽取数据类型不存在！" + extractType)


            # 配置指定抽取的工具
            if (extractTool == ExtractMysql.MYSQL_DUMP):
                  extractMysql.setExtractTool(ExtractMysql.MYSQL_DUMP)
            elif (extractTool == ExtractMysql.SQOOP) :
                 extractMysql.setExtractTool(ExtractMysql.SQOOP)
                 extractMysql.setMapReduceNum(5)


            # 设置抽取表的信息
            sourceDb = dbName
            sourceTable = tbName
            targetDb = affirmTargetDb
            targetTable = affirmTargetTb

            extractMysql.setTbId(tbId)
            extractMysql.setSourceDb(sourceDb)
            extractMysql.setSourceTable(sourceTable)
            extractMysql.setTargetDb(targetDb)
            extractMysql.setTargetTable(targetTable)
            extractMysql.run()

        except Exception,ex:
            log = "异常->  数据表: " + str(dbServer) + ": " + str(dbName) + "." + str(tbName) 
            log += " -> " + str(Exception) + ":" + str(ex)
            Logger.info(log)



    # 独立抽取数据表,使用默认的抽取规则算法抽取数据
    def extractMysqlTableIndependent(self, tableInfo):
        # 抽取
        extractMysql = ExtractMysql()

        # 源数据信息
        dbServer = tableInfo.get('dbServer')
        sourceDb = tableInfo.get('sourceDb')
        sourceTb = tableInfo.get('sourceTb')
        targetDb = tableInfo.get('targetDb')
        targetTb = tableInfo.get('targetTb')
        # 抽取工具
        extractTool = tableInfo.get('extractTool')
        # 抽取类型
        extractType = tableInfo.get('extractType')
        # 默认 1 ,三元表达式
        mapReduceNum = tableInfo.get('mapReduceNum') and tableInfo.get('mapReduceNum') or 1

        # 业务 数据库服务器
        # ExtractMysql.PRODUCE_DB
        extractMysql.setExtractDb(dbServer)

        # 抽取类型,全量
        extractMysql.setExtractType(ExtractMysql.COMPLETE)

        # 指定工具抽取
        extractMysql.setExtractTool(int(extractTool))

        # 抽取类型
        extractMysql.setExtractType(int(extractType))

        # sqoop 抽取的时候 mapreduce 数量
        extractMysql.setMapReduceNum(int(mapReduceNum))

        # Dump 方式时的保存目录
        extractMysql.setDumpFileDir('/data/log/mysql')

        extractMysql.setSourceDb(sourceDb)
        extractMysql.setSourceTable(sourceTb)
        extractMysql.setTargetDb(targetDb)
        extractMysql.setTargetTable(targetTb)
        extractMysql.run()


