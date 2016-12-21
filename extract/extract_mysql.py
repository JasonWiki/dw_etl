#coding=utf-8
from dw_service_core import DwServiceCore
from core.util.log.logger import Logger


u'''
Mysql 数据抽取

'''

class ExtractMysql(DwServiceCore):

    # MYSQL DUMP 最大文件大小边界，当超过这个值就会使用 Sqoop 导入
    # 500 MB
    #DUMP_SIZE_RANGE = 524288000

    # 300 MB
    #DUMP_SIZE_RANGE = 314572800

    # 200 MB
    DUMP_SIZE_RANGE = 209715200

    # 100MB
    #DUMP_SIZE_RANGE = 104857600

    # SQOOP 方式最大文件大小
    SQOOP_SIZE_RANGE = 314572800
    #SQOOP_SIZE_RANGE = 1048576000


    # 数据库服务器
    # 业务数据库
    PRODUCE_DB = 'product'
    # dw 数据仓库数据库
    DW_DB = 'dw'
    # 当前抽取的数据库标识
    extractDb = None
    # 当前抽取的 Model 对象
    extractDbServerModel = None
    # 设置 抽取服务器的信息
    def setExtractDb(self,dbServer):
        if (dbServer == ExtractMysql.PRODUCE_DB):
            self.extractDbServerModel = self.getRegisterInstance('produceDbModel')
        elif (dbServer ==  ExtractMysql.DW_DB):
            self.extractDbServerModel = self.getRegisterInstance('biDbModel')

        self.extractDb = dbServer
    def getExtractDb(self):
        return self.extractDb


    # 抽取方式
    # 全量
    COMPLETE = 1
    # 增量
    INCREMENTAL = 2
    # 当前选择方式
    extractType = None
    def setExtractType(self,data):
        self.extractType = data
    def getExtractType(self):
        return self.extractType


    # 抽取工具
    # dump 文件方式
    MYSQL_DUMP = 1
    # sqoop 导入方式
    SQOOP = 2
    # 当前选择方式
    extractTool = 0
    def setExtractTool(self,data):
        self.extractTool = data
    def getExtractTool(self):
        return self.extractTool


    # 表 id
    tbId = 0
    def setTbId(self, tbId):
        self.tbId = tbId
    def getTbId(self):
        return self.tbId


    # 源数据库
    sourceDb = None
    def setSourceDb(self,data):
        self.sourceDb = data
    def getSourceDb(self):
        return self.sourceDb

    # 源数据表
    sourceTable = None
    def setSourceTable(self,data):
        self.sourceTable = data
    def getSourceTable(self):
        return self.sourceTable


    # 目标数据库
    targetDb = None
    def setTargetDb(self,data):
        self.targetDb = data
    def getTargetDb(self):
        return self.targetDb

    # 目标数据表
    targetTable = None
    def setTargetTable(self,data):
        self.targetTable = data
    def getTargetTable(self):
        return self.targetTable


    # dump 的目录
    dumpFileDir = '/data/log/mysql'
    def setDumpFileDir(self,data):
        self.dumpFileDir = data
    def getDumpFileDir(self):
        return self.dumpFileDir


    # dump 的文件名
    dumpFileName = None
    def setDumpFileName(self,data):
        self.dumpFileName = data
    def getDumpFileName(self):
        if (self.dumpFileName == None):
            self.dumpFileName = self.getSourceDb() + "." + self.getSourceTable()
        return self.dumpFileName


    # MapReduce 数量
    mapReduceNum = None
    def setMapReduceNum(self,data):
        self.mapReduceNum = data
    def getMapReduceNum(self):
        if (self.mapReduceNum == None):
            self.mapReduceNum = 1
        return self.mapReduceNum
    
    
    # 表大小
    tbSize = None
    def setTbSize(self,data):
        self.tbSize = data
    def getTbSize(self):
        if (self.tbSize == None):
            self.tbSize = 0
        return self.tbSize


    # 表行数
    tbRows = None
    def setTbRows(self,data):
        self.tbRows = data
    def getTbRows(self):
        if (self.tbRows == None):
            self.tbRows = ''
        return self.tbRows



    u'''---------- 全量抽取处理 START ---------- '''

    # 获取待导入数据表信息
    def getSourceTableInfo(self):
        # 表大小
        tbInfo = self.extractDbServerModel.tableInfo(self.getSourceDb(),self.getSourceTable())
        self.setTbSize(tbInfo['tbSize'])

        # 表行数
        tbRows = self.extractDbServerModel.count("SELECT COUNT(*) AS c  FROM " + self.getSourceDb() + '.' + self.getSourceTable())
        self.setTbRows(tbRows)


    # 全量抽取控制
    def extractCompleteAction(self):

        # 当没有自定义抽取的工具,则使用系统默认规则
        if (self.getExtractTool() == 0):
            self.setExtractTool(ExtractMysql.MYSQL_DUMP)
            u'''
            self.getSourceTableInfo()

            # 如果小于边界值
            if (self.getTbSize() <= ExtractMysql.DUMP_SIZE_RANGE):
                # 设置抽取方式 :  mysql dump 工具抽取
                self.setExtractTool(ExtractMysql.MYSQL_DUMP)

            # 大于边界值时 Sqoop 抽取
            elif (self.getTbSize() >= ExtractMysql.DUMP_SIZE_RANGE and self.getTbSize() <= ExtractMysql.SQOOP_SIZE_RANGE) :
                # 设置抽取方式 :  sqoop 方式抽取
                self.setExtractTool(ExtractMysql.SQOOP)
                self.setMapReduceNum(5)

            # 超过 边界值 则加大 mpreduce 数量用 Sqoop 抽取
            elif (self.getTbSize() > ExtractMysql.SQOOP_SIZE_RANGE) :
                # 设置抽取方式 : sqoop 方式抽取
                self.setExtractTool(ExtractMysql.SQOOP)
                self.setMapReduceNum(10)
            '''
        # 执行全量抽取
        self.extractComplete()


    # 全量抽取工具选择
    def extractComplete(self):
        # Mysql Dump 抽取
        if (self.getExtractTool() == ExtractMysql.MYSQL_DUMP):
            self.extractMysqlDump()
        elif (self.getExtractTool() == ExtractMysql.SQOOP):
            self.extractMysqlSqoop()


    # MySQL Dump 全量方式抽取实现
    def extractMysqlDump(self):
        Logger.info("---------- mysqlDump 开始 " + str(self.getTbId()) + ': ' + str(self.getSourceDb()) + "." + str(self.getSourceTable()) + " ---------- ")
        # 开始时间
        startTimestamp = self.getRegisterInstance('dateModel').getTimestamp()

        # 1. 删除目标 Hive 对应表
        Logger.info("删除目标 Hive 对应表")
        self.getRegisterInstance('hiveModel').dropTable(self.getTargetDb() + "." + self.getTargetTable())

        # 2. dump mysql 数据到文件中
        Logger.info("dump mysql 数据到文件中")
        dumpSql = "SELECT * FROM " + self.getSourceDb() + "." + self.getSourceTable()
        dumpFile = self.getDumpFileDir() + "/" + self.getDumpFileName()
        dumpResult = self.extractDbServerModel.mysqlDumpFile(dumpSql ,dumpFile)

        # 3. 根据 Mysql 表结构创建 Hive 表结构
        Logger.info("根据 Mysql 表结构创建 Hive 表结构")
        sourceTableFields = self.getSourceTableFields()
        formatTableFieldsList = []
        for curField in sourceTableFields:
            formatTableFieldsList.append('`' + curField + '`')
        formatTableFieldsStr = ' String,'.join(formatTableFieldsList) + " String"

        createHiveTableSql = '''
CREATE TABLE IF NOT EXISTS  %s.%s (
%s
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\001'
COLLECTION ITEMS TERMINATED BY '\\n'
STORED AS TEXTFILE
'''%(self.getTargetDb(),self.getTargetTable(),formatTableFieldsStr)

        # 执行 Hive 创建表
        Logger.info("执行 Hive 创建表")
        createHiveTableResult = self.getRegisterInstance('hiveModel').createTable(createHiveTableSql)

        # 4. 上传 dump 文件到 HiveTable 中
        Logger.info("上传 dump 文件到 HiveTable 中")
        hiveLoadSql =  "LOAD DATA LOCAL INPATH '" + self.getDumpFileDir() + "/" + self.getDumpFileName() + "' OVERWRITE INTO TABLE " + self.getTargetDb() + "." + self.getTargetTable() + ";"
        hiveLoadResult = self.getRegisterInstance('hiveModel').runHiveScript(hiveLoadSql)

        # 5. 检测执行结果
        if (dumpResult['code'] == 0 and createHiveTableResult == True and hiveLoadResult['code'] == 0 ) :
            resultCode = 0
        else:
            resultCode = hiveLoadResult['code']

        # 6. 计算执行结果写日志和打印
        # 计算结束日期
        diffTimestamp = self.getRegisterInstance('dateModel').getTimestamp() - startTimestamp

        # mysql 记录日志
        self.extractLog(resultCode,diffTimestamp)

        # 打印日志
        logStr = "全量抽取 : (Dump : " + str(self.getTbId()) + ': ' + str(self.getSourceDb()) + "." + str(self.getSourceTable()) + " -> " + str(self.getTargetDb()) +  "." + str(self.getTargetTable()) + " Time : " + str(diffTimestamp) + ")"
        Logger.info(logStr)



    # sqoop 方式抽取
    def extractMysqlSqoop(self):
        Logger.info("---------- sqoop 开始 "  + str(self.getTbId()) + ': ' + str(self.getSourceDb()) + "." + str(self.getSourceTable()) +  " ----------")
        
        # 开始时间
        startTimestamp = self.getRegisterInstance('dateModel').getTimestamp()

        # 1. 删除目标 Hive 对应表
        Logger.info("删除目标 Hive 对应表")
        self.getRegisterInstance('hiveModel').dropTable(self.getTargetDb() + "." + self.getTargetTable())

        # 2. 执行 Sqoop Mysql 导入到 Hive
        Logger.info("执行 Sqoop Mysql 导入到 Hive")
        self.getRegisterInstance('sqoopModel').setDbServer(self.getExtractDb())
        self.getRegisterInstance('sqoopModel').setSourceDb(self.getSourceDb())
        self.getRegisterInstance('sqoopModel').setSourceTable(self.getSourceTable())
        self.getRegisterInstance('sqoopModel').setTargetDb(self.getTargetDb())
        self.getRegisterInstance('sqoopModel').setTargetTable(self.getTargetTable())
        self.getRegisterInstance('sqoopModel').setMapReduceNum(self.getMapReduceNum())
        result = self.getRegisterInstance('sqoopModel').importMysqlToHive()

         # 3. 检测执行结果
        if (result['code'] == 0):
            resultCode = 0
        else :
            resultCode = result['code']

        # 4. 计算执行结果写日志和打印
        # 计算结束日期
        diffTimestamp = self.getRegisterInstance('dateModel').getTimestamp() - startTimestamp

        #mysql 记录日志
        self.extractLog(resultCode,diffTimestamp)

        # 打印日志
        logStr = "全量抽取 : (Sqoop : " + str(self.getTbId()) + ': ' + str(self.getSourceDb()) + "." + str(self.getSourceTable()) + " -> " + str(self.getTargetDb()) +  "." + str(self.getTargetTable()) + " Time : " + str(diffTimestamp) + ")"
        Logger.info(logStr)
    
    u'''---------- 全量抽取处理 END ---------- '''



    u'''---------- 增量抽取处理 START ---------- '''

    # 设置增量表属性
    incrementalAttribute = {}
    def setIncrementalAttribute(self, data):
        self.incrementalAttribute = data
    def getIncrementalAttribute(self):
        return self.incrementalAttribute


    # 增量抽取流程控制
    def extractIncrementalAction(self):

        # 检测 hive 数据表是否存在
        if (self.isExistsHiveTable() == False):
            Logger.info("增量抽取控制: 初始化, 全量抽取...  -> " + str(self.getTbId()) + ': ' + str(self.getSourceDb()) + "." + str(self.getSourceTable()) ) 
            # 不存在全量抽取
            self.extractCompleteAction()
        else:
            # 检测字段变化
            # 有变化时
            if (self.checkStbAndTtbFields() == True):
                Logger.info("增量抽取控制: 结构发生变化, 初始化, 全量抽取...  -> " + str(self.getTbId()) + ': ' + str(self.getSourceDb()) + "." + str(self.getSourceTable()) ) 
                # 全量抽取
                self.extractCompleteAction()
            # 无变化
            else:
                Logger.info("增量抽取控制: 增量抽取...  -> " + str(self.getTbId()) + ': ' + str(self.getSourceDb()) + "." + str(self.getSourceTable())) 
                # 增量抽取
                self.extractIncrementalTable()


    # 增量抽取方法实体
    def extractIncrementalTable(self):
        Logger.info("---------- 增量抽取开始 "  + str(self.getTbId()) + ': ' + str(self.getSourceDb()) + "." + str(self.getSourceTable()) +  " ----------")
        
         # 开始时间
        startTimestamp = self.getRegisterInstance('dateModel').getTimestamp()

        # 设置增量表属性
        Logger.info("设置增量表属性")
        tableInfoExt = self.getRegisterInstance('biDbModel').getExtractMysqlTableExt(self.getTbId())
        self.setIncrementalAttribute(tableInfoExt)

        # 获取增量表的属性
        Logger.info("获取增量表的属性")
        incTbAttr = self.getIncrementalAttribute()
        primaryKey = incTbAttr['primary_key']
        incrementalField = incTbAttr['incremental_field']
        incrementalVal = incTbAttr['incremental_val']
        conditions = incTbAttr['conditions']

        # hive 目标表
        targetTb = self.getTargetDb() + "." + self.getTargetTable()
        # hive 增量表
        incTb = targetTb + "__inc"


        # 1. 删除增量抽取表 
        Logger.info("删除增量抽取表")
        self.getRegisterInstance('hiveModel').dropTable(incTb)

        # 2. 创建增量抽取表
        Logger.info("创建增量抽取表")
        createHiveTableSql = "CREATE TABLE " + incTb + " LIKE " + targetTb
        createHiveTableResult = self.getRegisterInstance('hiveModel').createTable(createHiveTableSql)

        incDumpSql = ""
        # 3. 读取最新的增量数据到本地文件中
        if (incrementalVal == ""):
            # 获取目标表，最大的字段数, 已这个作为基地抽取数据
            Logger.info("获取目标表，最大的字段数, 已这个作为基地抽取数据")
            targetTbMaxPointVal = self.getHiveTbMaxVal(targetTb, incrementalField)

            incDumpSql = "SELECT * FROM " + self.getSourceDb() + "." + self.getSourceTable() + " WHERE " + incrementalField + conditions + "'" + str(targetTbMaxPointVal) + "'"

            # 更新 point 点
            Logger.info("更新 point 点")
            self.updateTableExt(tableInfoExt['id'], targetTbMaxPointVal)
        else:
            incDumpSql = "SELECT * FROM " + self.getSourceDb() + "." + self.getSourceTable() + " WHERE " + incrementalField + conditions + "'" + incrementalVal + "'" 

        Logger.info("dump 更新数据到本地")
        dumpIncFile = self.getDumpFileDir() + "/" + self.getDumpFileName() + ".inc"
        dumpIncResult = self.extractDbServerModel.mysqlDumpFile(incDumpSql ,dumpIncFile)


        # 4. 上传 dump 文件到 incHiveTable 中
        Logger.info("上传 dump 文件到 incHiveTable 中")
        hiveLoadSql =  "LOAD DATA LOCAL INPATH '" + dumpIncFile + "' OVERWRITE INTO TABLE " + incTb + ";"
        hiveLoadResult = self.getRegisterInstance('hiveModel').runHiveScript(hiveLoadSql)


        # 获取增量表本次最大一条增量的增量字段值
        Logger.info("获取增量表本次最大一条增量的增量字段值")
        incTbMaxPointVal = self.getHiveTbMaxVal(incTb, incrementalField)
        # 当抽取的增量数据为空时, 不做任何处理, 直接退出
        if (incTbMaxPointVal == None):
            Logger.info("增量数据为空...")
            return 

        # 5. 执行存储过程完成增量
        incHiveSql = """
INSERT OVERWRITE TABLE %(targetTb)s
SELECT *
FROM (
    SELECT a.*
    FROM %(targetTb)s AS a
    LEFT JOIN %(incTb)s AS b
        ON a.%(primaryKey)s = b.%(primaryKey)s
    WHERE b.%(primaryKey)s IS NULL
) AS bs

UNION ALL
SELECT * FROM %(incTb)s
;"""%{'targetTb': targetTb,
      'incTb': incTb,
      'primaryKey': primaryKey
      }

        Logger.info(incHiveSql)

        # 6. 最终逻辑运算使用 spark sql
        Logger.info("最终逻辑运算使用 spark sql")
        incSqlResult = self.getRegisterInstance('sparkModel').batchExecuteSql(incHiveSql)
        incSqlResult= True

        # 7. 检测执行结果
        if (dumpIncResult['code'] == 0 
            and hiveLoadResult['code'] == 0
            and createHiveTableResult == True 
            and incSqlResult == True ) :

            # 更新节点
            if (incTbMaxPointVal != None):

                self.updateTableExt(tableInfoExt['id'],incTbMaxPointVal )

            resultCode = 0
        else:
            resultCode = 1


        # 计算执行结果写日志和打印
        # 计算结束日期
        diffTimestamp = self.getRegisterInstance('dateModel').getTimestamp() - startTimestamp

        # mysql 记录日志
        self.extractLog(resultCode, diffTimestamp)

        # 打印日志
        logStr = "增量抽取 : (Dump : " + str(self.getTbId()) + ': ' + str(self.getSourceDb()) + "." + str(self.getSourceTable()) + " -> " + str(self.getTargetDb()) +  "." + str(self.getTargetTable()) + " Time : " + str(diffTimestamp) + ")"
        Logger.info(logStr)


    u'''---------- 增量抽取处理 END ---------- '''



    u''' ---------- TOOLS ---------'''

    # 获取 Mysql 字段
    def getSourceTableFields(self):
        return self.extractDbServerModel.getFileds(self.getSourceDb(),self.getSourceTable())

    # 记录日志到 Mysql
    def extractLog(self,code,time):
        logSql = "INSERT INTO dw_service.extract_log (`db_server`,`db_name`,`tb_name`,`extract_type`,`extract_tool`,`code`,`run_time`,`size`,`rows`) "
        logSql += "VALUES ('%s','%s','%s',%d, %d, %d, %d, '%s','%s')"
        #print self.getExtractDb(),self.getSourceDb(),self.getSourceTable(),self.getExtractType(),self.getExtractTool(),code,time,self.getTbSize(),self.getTbRows()
        logSql = logSql%(self.getExtractDb(),self.getSourceDb(),self.getSourceTable(),self.getExtractType(),self.getExtractTool(),code,time,self.getTbSize(),self.getTbRows())
        self.getRegisterInstance('biDbModel').insertData(logSql)

    
     # 检测 hive target 数据表是否存在
    def isExistsHiveTable(self):
        return self.getRegisterInstance('sparkModel').isExistsTable(self.getTargetDb(),self.getTargetTable())


    # source table 对比 target table 后, 字段是否发生变化
    def checkStbAndTtbFields(self):
        # False 未变化, True 已变化
        status = False

        # mysql 源表字段
        sourceTableFields = self.getSourceTableFields()
        # hive 表字段
        gatherTableFields = self.getRegisterInstance('sparkModel').getFileds(self.getTargetDb(),self.getTargetTable())

        #获取新增的字段
        changeFileds = []

        # 字段长度不同表示变化
        if (len(sourceTableFields) != len(gatherTableFields)):
            status = True
        else:
            # 对比 mysql 字段与 hive 字段, 发生变换的字段
            for sourceField in sourceTableFields:
                if (sourceField not in gatherTableFields):
                    changeFileds.append(sourceField)

            # 变换的字段数量
            if len(changeFileds) > 0: 
                status = True

        return status



    # 获取 hive 表某个字段最大值
    def getHiveTbMaxVal(self, tb, field = ""):

        if (field.find("id") >= 0):
            tbMaxSql = "SELECT MAX(int(" + field + ")) AS c FROM " + tb
        else:
            tbMaxSql = "SELECT MAX(" + field + ") AS c FROM " + tb

        tbMaxVal = self.getRegisterInstance('sparkModel').queryMax(tbMaxSql)

        return tbMaxVal



    # 更新扩展表信息
    def updateTableExt(self, tbId, incVal):
        updateTableExtSql = "UPDATE dw_service.extract_table_ext SET incremental_val='" + str(incVal) +"' WHERE id='" + str(tbId) + "'"
        return self.getRegisterInstance('biDbModel').updataData(updateTableExtSql)

    u''' ---------- TOOLS ---------'''




    u''' 入口 '''

    def run(self):
        Logger.init()

        # 抽取类型(全量、增量)
        curExtractType = self.getExtractType()

        # 全量抽取
        if (curExtractType == ExtractMysql.COMPLETE):
            self.extractCompleteAction()
        # 增量抽取
        elif(curExtractType == ExtractMysql.INCREMENTAL):
            self.extractIncrementalAction()


