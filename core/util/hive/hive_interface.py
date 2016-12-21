#coding=utf-8
from core.util.hive.hive_server2 import HiveServer2
from core.util.base.process import Process
from time import ctime,sleep


class HiveInterface:

    hiveConf = {}
    def __init__(self,data):
        self.hiveConf = data
        #self.__setHiveConnection(self.hiveConf)


    u'''
    hive 操作数据库对象接口,私有有方法，请不要在类外使用
    '''
    __hiveConnection = None
    def __setHiveConnection(self,conf):
        self.__hiveConnection = HiveServer2(conf)
    def __getHiveConnection(self):
        #return self.__hiveConnection
        self.__setHiveConnection(self.hiveConf)
        return self.__hiveConnection


    u'''
    创建数据表
    返回 bool 值
    '''
    def createTable(self,createTableSql):
        return self.__getHiveConnection().execute([createTableSql])

    u'''
    删除表
    返回 bool 值
    '''
    def dropTable(self,dbTbName):
        dropTableSql = "DROP TABLE " + dbTbName;
        if (self.__getHiveConnection().execute([dropTableSql]) == True):
            return True
        else:
            return False


    u'''
    删除数据库 ，谨慎操作！
    返回 bool 值
    '''
    def dropDb(self,dbName):
        dropDbNameSql = "DROP DATABASE IF EXISTS " + dbName;
        if (self.__getHiveConnection().execute([dropDbNameSql]) == True):
            return True
        else:
            return False


    u'查询数据记录条数'
    def queryCount(self,sql): 
        queryData =  self.__getHiveConnection().query([sql])
        return queryData[0][0]
    

    u'查询最大记录数'
    def queryMax(self,sql): 
        queryData =  self.__getHiveConnection().query([sql])
        return queryData[0][0]


    u'查询数据'
    def query(self,sql,isShowField = True):
        result = None
        if (isShowField == True):
            result = self.__getHiveConnection().queryReturnField([sql])
        else:
            result = self.__getHiveConnection().query([sql]);
        return result


    u'显示指定数据库表'
    def getDbTables(self,dbName):
        queryData = self.__getHiveConnection().query(["USE " + dbName,"SHOW TABLES"])

        result = []
        for curTable in queryData:
            result.append(curTable[0])
        return result


    u'验证 Table 是否存在'
    def isExistsTable (self,dbName,tbName):
        try :
            self.query('DESC ' + dbName + '.' +  tbName, False)
            result = True
        except Exception,ex:
            result = False

        return result


    u'获取数据表字段 set hive.display.partition.cols.separately=false' 
    def getFileds(self,dbName,tbName):

        sqlList = [
            'SET hive.display.partition.cols.separately=false',
            "DESC " + dbName  + "." + tbName
        ]
        data = self.__getHiveConnection().queryReturnField(sqlList)

        fields = []
        for i in data:
            fields.append(i.get('col_name'))

        return fields


    u'''
    批量执行 sql 语句
    '''
    def batchExecuteSql(self,sqlContent):
        sqlContentList = sqlContent.split(';');
        formatSqlContentList = sqlContentList[:-1];

        return self.__getHiveConnection().execute(formatSqlContentList)


    u'命令行使用本地 hive 脚本'
    def runHiveScript(self,sql):
        hiveHome = self.hiveConf['hiveHome']
        runCommand = hiveHome + "/bin/hive -e " + '\"' + sql + '\"'
        result = Process.runScriptSync(runCommand)
        return result



    u'''
    test hive 执行脚本命令接口
    '''
    def hiveBinInterface(self):
        #process = Process()
        #result =  process.runScript('ls ~/develop/jason/dw_etl/dw_service ')
        #print result['stdoutPut']
        
        #list = ['ls ~/develop/jason/dw_etl/dw_service','ls ~/develop/jason/uba']
        #list =  ['hadoop dfs -ls /user/hive','hadoop dfs -ls /user']
        list = ['hive -e "select count(*) from dw_db.dw_broker_summary_basis_info_daily;"',
                'hive -e "select count(*) from dw_db.dw_cal;"']
        result = Process.runThreadingScripts(list)

        status = True
        while (status):
            sleep(1)
            for item in result:
                print item
                u'获取当前活动的(alive)线程的个数'
                print item.isAlive()