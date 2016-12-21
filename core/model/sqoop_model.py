#coding=utf-8
from core.core import Core
from core.model.conf_model import ConfModel
from core.util.base.process import Process


u'''
Sqoop Model
'''
class SqoopModel:

    # 业务 DB 服务配置
    DB_CONF = {}

    systemConf = {}
    systemCorePath = {}

    def __init__(self):
        self.systemConf =  ConfModel.getSystemConf()
        self.systemCorePath = ConfModel.getCoreSystemPath()

    # 数据库服务器
    # 业务数据库
    PRODUCT_DB = 'product'
    # dw 数据仓库数据库
    DW_DB = 'dw'
    # 当前抽取的数据库服务器
    dbServer = None
    # 当前数据库指定 Model
    dbServerModel = None
    def setDbServer(self,dbServer):
        if (dbServer == SqoopModel.PRODUCT_DB):
            self.dbServer = {
                'mysql_host' : self.systemConf['mysql_produce_db']['host'],
                'mysql_user' : self.systemConf['mysql_produce_db']['user'],
                'mysql_password' : self.systemConf['mysql_produce_db']['password'],
                'mysql_port' : self.systemConf['mysql_produce_db']['port'],
            }
            self.dbServerModel = Core.getModelInterface('ProduceDb')
        elif (dbServer ==  SqoopModel.DW_DB):
            self.dbServer = {
                'mysql_host' : self.systemConf['mysql_bi_db']['host'],
                'mysql_user' : self.systemConf['mysql_bi_db']['user'],
                'mysql_password' : self.systemConf['mysql_bi_db']['password'],
                'mysql_port' : self.systemConf['mysql_bi_db']['port'],
            }
            self.dbServerModel = Core.getModelInterface('BiDb')
    def getDbServer(self):
        return self.dbServer


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


    # MapReduce 数量,默认 1 个
    mapReduceNum = 1
    def setMapReduceNum(self,data):
        self.mapReduceNum = data
    def getMapReduceNum(self):
        return self.mapReduceNum


    # 分割字符串,默认 \001 分割
    fieldsTerminated = "\\001"
    def setFieldsTerminated(self,data):
        self.fieldsTerminated = data
    def getFieldsTerminated(self):
        return self.fieldsTerminated


    # 导入 mysql
    def importMysqlToHive(self):

        # 当前选择数据库服务器
        dbServer = self.getDbServer()

        # 获取 带抽取的 Mysql 表结构 ,用来创建 Hive 数据表
        mysqlFileds = self.dbServerModel.getFileds(self.getSourceDb(),self.getSourceTable())
        mysqlFiledsFormat = '=String,'.join(mysqlFileds) + "=String"

        # 执行脚本
        script = self.systemCorePath['shellPath'] + '/sqoop_import_mysql.sh '
        # 参数
        script += '--sqoop_home \"'+self.systemConf['sqoop']['sqoop_home']+'\" '
        script += '--local_tmp_dir \"'+self.systemCorePath['tmpPath']+'\" '
        script += '--hdfs_sqoop_tmp_dir \"/tmp/sqoop\" '
        script += '--mysql_host \"'+dbServer['mysql_host']+'\" '
        script += '--mysql_port \"'+dbServer['mysql_port']+'\" '
        script += '--mysql_user \"'+dbServer['mysql_user']+'\" '
        script += '--mysql_password \"'+dbServer['mysql_password']+'\" '
        script += '--mysql_database \"'+self.getSourceDb()+'\" '
        script += '--mysql_table \"'+self.getSourceTable()+'\" '
        script += '--hive_table \"'+self.getTargetDb()+'.'+self.getTargetTable()+'\" '
        script += '--fields_terminated_by \"'+self.getFieldsTerminated()+'\" '
        script += '--map_column_hive_fields \"'+mysqlFiledsFormat+'\" '
        script += '--mappers_num \"'+str(self.getMapReduceNum())+'\"'

        result = Process.runScriptSync(script)
        return result


    # hbase 指定 row_key
    hbaseRowKey = "row_key"
    def setHbaseRowKey(self,data):
        self.hbaseRowKey = data
    def getHbaseRowKey(self):
        return self.hbaseRowKey

    # hbase 指定 column_family
    hbaseColumnFamily = "row_key"
    def setHbaseColumnFamily(self,data):
        self.hbaseColumnFamily = data
    def getbaseColumnFamily(self):
        return self.hbaseColumnFamily

    # 导入 mysql 到 table
    def importMysqlToHbase(self, querySql = None):
        # 当前选择数据库服务器
        dbServer = self.getDbServer()

        tmpDir = self.systemCorePath['tmpPath'] + '/sqoop_outdir'
        targetDir = '/tmp/sqoop/' + self.getTargetTable()

        # 执行脚本
        script = self.systemConf['sqoop']['sqoop_home'] + '/bin/sqoop import '
        script += '--connect \"jdbc:mysql://' + dbServer['mysql_host'] + ':' + dbServer['mysql_port'] + '/' + self.getSourceDb() + '?useUnicode=true&tinyInt1isBit=false&characterEncoding=utf-8\" ' 
        script += '--username \"'+dbServer['mysql_user']+'\" '
        script += '--password \"'+dbServer['mysql_password']+'\" '
        script += '--hbase-create-table '
        script += '--hbase-table \"'+self.getTargetTable()+'\" '
        script += '--column-family \"'+self.getbaseColumnFamily()+'\" '
        script += '--m \"'+str(self.getMapReduceNum())+'\" '
        script += '--outdir \"'+ tmpDir +'\" '
        script += '--target-dir \"' + targetDir + '\" '
        script += '--delete-target-dir '

        u' 表示整张表导入'
        if (querySql == None): 
            rmTmpTable = 'rm ' + tmpDir + '/' + self.getSourceTable() + '.java'
            Process.runScriptSync(rmTmp)

            script += '--table \"'+self.getSourceTable()+'\" '
        else :
            u' 清理临时文件'
            rmTmp = 'rm ' + tmpDir + '/QueryResult.java'
            Process.runScriptSync(rmTmp)

            u' 表示使用查询语句导入'
            script += '--hbase-row-key \"'+self.getHbaseRowKey()+'\" '
            script += '--split-by \"'+self.getHbaseRowKey()+'\" '
            script += '--query \"'+ querySql +'\"'

        #print script
        result = Process.runScriptSync(script)
        return result

