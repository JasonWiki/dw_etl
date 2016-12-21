#coding=utf-8

u'''
Hive Server2 通讯类，文档地址
https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2#SettingUpHiveServer2-PythonClientDriver
'''

import pyhs2

class HiveServer2:

    u'''
    构造方法，配置通讯属性
    par
     dict = {
           'host':'uhadoop-ociicy-master2',
           'port':10000,
           'user':'dwadmin',
           'password':'dwadmin',
       }
    '''
    def __init__(self,dict = {}):
        self.setConnectionBase(dict)
        self.setConnection()


    u'''
    连接属性
    '''
    connectionBase = {}
    def setConnectionBase(self,dict):
        dict.setdefault('authMechanism',"PLAIN")
        dict.setdefault('database','default')
        self.connectionBase = dict

    def getConnectionBase(self,key = None):
        if (key == None):
            return self.connectionBase
        else:
            return self.connectionBase[key]


    u'''
    设置链接
    '''
    connection = None
    def setConnection(self):
        try :
            self.connection = pyhs2.connect(
                host=self.getConnectionBase('host'),
                port=self.getConnectionBase('port'),
                authMechanism=self.getConnectionBase('authMechanism'),
                user=self.getConnectionBase('user'),
                password=self.getConnectionBase('password'),
                database=self.getConnectionBase('database')
            )
        except Exception,ex:
            print 'hiveServer2 创建连接失败：' +  Exception,ex

    u'''
    获取连接句对象
    '''
    def getConnection(self):
        return self.connection


    u''' 
    负责执行 Sql
    @par sql
    @result bool
    '''
    def execute(self,sqls = []):
        cursor = self.getConnection().cursor()
        try:
            for cur_sql in sqls:
                cursor.execute(cur_sql)
            status = True
        except Exception,ex:
            print Exception,":",ex
            status = False

        return status


    u'''
    查询返回,并且返回查询字段
    '''
    def queryReturnField (self,sqls = []):
        cursor =  self.getConnection().cursor()


        for cur_sql in sqls:
            cursor.execute(cur_sql)
    
        u'返回查询表字段名称'
        fields = []
        for cur_field_info in cursor.getSchema():
             fields.append(cur_field_info['columnName'])
    
        u'返回查询表字段名称'
        result = []
        for cur_row in cursor.fetch():
            result.append(dict(zip(fields,cur_row)))


        return result

    u'''
    查询，不返回字段
    '''
    def query(self,sqls = []):
        cursor =  self.getConnection().cursor()

        for cur_sql in sqls:
            cursor.execute(cur_sql)

        return cursor.fetch()

    u'''
    Show databases
    '''
    def getDatabases(self):
        return self.getConnection().cursor().getDatabases()

