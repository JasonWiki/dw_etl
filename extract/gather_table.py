#coding=utf-8
from dw_service_core import DwServiceCore
from core.util.log.logger import Logger

class GatherTable(DwServiceCore):

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


    # 分区日期
    partitionDate = None
    def setPartitionDate(self,data):
        self.partitionDate = data
    def getPartitionDate(self):
        return self.partitionDate
    

    # 验证 Gather Table 是否存在
    def isExistsGatherTable(self):
        return self.getRegisterInstance('hiveModel').isExistsTable(self.getTargetDb(),self.getTargetTable())



    # 初始化 gather table
    def initGatherTable(self):
        # 开始时间
        startTimestamp = self.getRegisterInstance('dateModel').getTimestamp()

        # 1. 根据源数据表，创建一张 gather table
        # 获取源数据表字段
        sourceTableFields = self.getRegisterInstance('hiveModel').getFileds(self.getSourceDb(),self.getSourceTable())
        formatTableFieldsList = []
        for curField in sourceTableFields:
            formatTableFieldsList.append('`' + curField + '`')
        formatTableFieldsStr = ' String,'.join(formatTableFieldsList) + " String"

        createHiveTableSql = '''
CREATE TABLE IF NOT EXISTS  %s.%s (
%s
) PARTITIONED BY  (
  `p_dt` String
)
STORED AS ORC
'''%(self.getTargetDb(),self.getTargetTable(),formatTableFieldsStr)


        # 创建数据表
        createHiveTableResult = self.getRegisterInstance('hiveModel').createTable(createHiveTableSql)

        # 2. 导入数据到 gather 表
        insertSql = '''
INSERT OVERWRITE TABLE `%(gatherTable)s` PARTITION (`p_dt` = '%(partitionDate)s') SELECT * FROM %(sourceTable)s;
'''% {'gatherTable':self.getTargetDb() + '.' + self.getTargetTable(),
       'partitionDate' : self.getPartitionDate(),
       'sourceTable' : self.getSourceDb() + '.' + self.getSourceTable()
      }
        # 执行导入
        insertResult = self.getRegisterInstance('hiveModel').batchExecuteSql(insertSql)

        # 3. 检测执行结果
        if (createHiveTableResult == True and insertResult == True ) :
            resultCode = 0
        else:
            resultCode = 1

        # 4. 计算执行结果写日志和打印
        # 计算结束日期
        diffTimestamp = self.getRegisterInstance('dateModel').getTimestamp() - startTimestamp

        # mysql 记录日志
        self.extractLog(resultCode,diffTimestamp)

        # 打印日志
        logStr = "(初始化 : " + str(self.getSourceDb()) + "." + str(self.getSourceTable()) + " -> " + str(self.getTargetDb()) +  "." + str(self.getTargetTable()) + " Time : " + str(diffTimestamp) + ")"
        Logger.info(logStr)


    # source table 对比 gather table 后, source table 新增的字段
    def getSourceTableNewFields(self):
        sourceTableFields = self.getRegisterInstance('hiveModel').getFileds(self.getSourceDb(),self.getSourceTable())
        gatherTableFields = self.getRegisterInstance('hiveModel').getFileds(self.getTargetDb(),self.getTargetTable())

        #获取新增的字段
        newFileds = []
        for sourceField in sourceTableFields:
            if (sourceField not in gatherTableFields):
                newFileds.append(sourceField)

        return newFileds


    # 修改 gather Table 表结构
    def alterGatherTableField(self,fields):
        alterTableSql = '';

        for field in fields:
             alterTableSql+='''ALTER TABLE `%(gatherTable)s` ADD COLUMNS(`%(field)s` String COMMENT '%(partitionDate)s');
'''%{'gatherTable':self.getTargetDb() + '.' + self.getTargetTable(),
       'field' : field,
       'partitionDate' : self.getPartitionDate()
    };

        return self.getRegisterInstance('hiveModel').batchExecuteSql(alterTableSql)


    #  source table 导入到 gather table 
    def sourceTableToGatherTable(self):
        # 开始时间
        startTimestamp = self.getRegisterInstance('dateModel').getTimestamp()

        # 1. 获取表结构
        sourceTableFields = self.getRegisterInstance('hiveModel').getFileds(self.getSourceDb(),self.getSourceTable())
        gatherTableFields = self.getRegisterInstance('hiveModel').getFileds(self.getTargetDb(),self.getTargetTable())

        # 2. 格式化需要导入到 gather table 的字段
        fieldSql = ''
        for curGatherField in gatherTableFields:
            if (curGatherField == 'p_dt') : continue
            
            if (curGatherField in sourceTableFields):
                fieldSql += '`' + curGatherField + '`,'
            else:
                fieldSql += "'' AS " + '`' + curGatherField + '`,'

        # 祛除最后的逗号
        formatFieldSql = fieldSql[:-1]

        # 3. 拼接 SQL
        gatherTableSql = '''
INSERT OVERWRITE TABLE `%(gatherTable)s` PARTITION (`p_dt` = '%(partitionDate)s') SELECT %(fieldSql)s FROM %(sourceTable)s;
'''% {'gatherTable':self.getTargetDb() + '.' + self.getTargetTable(),
       'partitionDate' : self.getPartitionDate(),
       'fieldSql' : formatFieldSql,
       'sourceTable' : self.getSourceDb() + '.' + self.getSourceTable()
      }
        # 执行 SQL
        gatherTableResult = self.getRegisterInstance('hiveModel').batchExecuteSql(gatherTableSql)

         # 4. 检测执行结果
        if (gatherTableResult == True ) :
            resultCode = 0
        else:
            resultCode = 1

        # 5. 计算执行结果写日志和打印
        # 计算结束日期
        diffTimestamp = self.getRegisterInstance('dateModel').getTimestamp() - startTimestamp

        # mysql 记录日志
        self.extractLog(resultCode,diffTimestamp)

        # 打印日志
        logStr = "(执行聚合 : " + str(self.getSourceDb()) + "." + str(self.getSourceTable()) + " -> " + str(self.getTargetDb()) +  "." + str(self.getTargetTable()) + " Time : " + str(diffTimestamp) + ")"
        Logger.info(logStr)


    # 记录日志到 Mysql
    def extractLog(self,code,time):
        logSql = "INSERT INTO dw_service.gather_log (`db_name`,`tb_name`,`code`,`run_time`) "
        logSql += "VALUES ('%s','%s',%d,%d)"
        logSql = logSql%(self.getSourceDb(),self.getSourceTable(),code,time)
        self.getRegisterInstance('biDbModel').insertData(logSql)


    def run(self):
        Logger.init()
        # 目标数据库不存在
        if (self.isExistsGatherTable() == False):
            # 初始化数据表
            self.initGatherTable()
        else:
            sourceTableNewFields = self.getSourceTableNewFields()
            # 如果 source table 有新增的字段
            if (len(sourceTableNewFields) > 0):
                # 增加新的字段
                self.alterGatherTableField(sourceTableNewFields)
                # 导入
                self.sourceTableToGatherTable()
            else:
                # 导入
                self.sourceTableToGatherTable()


