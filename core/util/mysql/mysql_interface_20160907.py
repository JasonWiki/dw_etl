#coding=utf-8
from core.util.mysql.mysql import MySql
from core.util.base.process import Process
from time import ctime,sleep


class MysqlInterface:

    mysqlConf = {}

    def __init__(self,data):
        self.mysqlConf = data
        self.__mysqlConnection = MySql(self.mysqlConf)

    def queryOne(self,sql):
        self.__mysqlConnection.query(sql)
        return self.__mysqlConnection.fetchOneRow();

    u'查询返回数据'
    def queryAll(self,sql):
        self.__mysqlConnection.query(sql)
        return self.__mysqlConnection.fetchAllRows();


    u'删除数据表'
    def dropTable(self,dbTbName):
        dropTableSql = "DROP TABLE IF EXISTS " + dbTbName;
        if (self.__mysqlConnection.update(dropTableSql) == True):
            return True
        else:
            return False

    u'获取数据表信息'
    def tableInfo(self,dbName,tbName):
        sql = "SELECT `table_schema`,`table_name`,`table_rows`, `data_length`,`row_format` FROM information_schema.tables WHERE `table_schema`='"+ dbName +"' AND `table_name`='"+ tbName +"' LIMIT 1;"
        self.__mysqlConnection.query(sql)
        data = self.__mysqlConnection.fetchOneRow()
        result = {
            'dbName' : data['table_schema'],
            'tbName' : data['table_name'],
            'tbRows' : data['table_rows'],
            'tbSize' : data['data_length'],
        }
        return result


    u'获取表字段'
    def getFileds(self,dbName,tbName):
        data = self.queryAll("DESC " + dbName  + "." + tbName)
        fields = []
        for i in data:
            fields.append(i.get('Field'))
        return fields


    u'插入数据'
    def insertData(self,sql):
        id = self.__mysqlConnection.insert(sql)
        if (id == True):
            return id
        else:
            return False


    u'修改数据,删除数据'
    def updataData(self,upSql):
        if (self.__mysqlConnection.update(upSql) == True):
            return True
        else:
            return False


    u'批量执行 sql'
    def batchExecuteSql(self,sqlContent):
        sqlContentList = sqlContent.split(';');
        formatSqlContentList = sqlContentList[:-1];

        status = self.__mysqlConnection.executeSqls(formatSqlContentList)

        return status

    u'''
    获取表行数
    使用方法
    SELECT COUNT(*) AS c  FROM  xxx
    '''
    def count(self,sql):
        self.__mysqlConnection.query(sql)
        data = self.__mysqlConnection.fetchOneRow()
        return data['c']


    u'获取命令行 Mysql 脚本执行参数'
    def getMysqlCommand(self):
        return "mysql -h" + str(self.mysqlConf['host']) + " -u" + str(self.mysqlConf['user']) +" -p" + str(self.mysqlConf['passwd']) + " -P" + str(self.mysqlConf['port'])


    u'命令行 执行 mysql SQL'
    def runMysqlCommand(self,sql = ''):
        scriptBase = self.getMysqlCommand() + " -N -s -e "
        runScript =  scriptBase + "\"" + sql + "\""  
        return Process.runScriptSync(runScript)
    
    # Mysql 查询语句 Dump 到本地文件中
    def mysqlDumpFile(self,sql,file):

        # sed 格式化规则
        # 处理行中的换行符号
        rowRegexp = 's/[\\n|\\r\\n]//g;'
        # 处理行中 NULL 字符串
        rowRegexp += 's/NULL/\\\\\N/g;'
        # 列分隔符 
        rowRegexp += 's/\t/\001/g;'

        # 组合命令
        script = self.getMysqlCommand() + " -N -s -e ""\"" + sql + "\" | sed -e \"" + rowRegexp + "\" > " + file

        result = Process.runScriptSync(script)

        return result

