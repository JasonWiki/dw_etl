#coding=utf-8

u'''
清理无用数据库，请谨慎操作！

适用方法：
    ./index.py --service cluster_task --module clean_db --parameter 'dbName' (数据库名称) 
'''

from dw_service_core import DwServiceCore


class CleanDb(DwServiceCore) :

    __dbName = None

    __tables = None

    def process(self):
        self.__dbName = self.getParameter()

        self.dropDbTables()


    u'删除数据库的所有表'
    def dropDbTables(self):

        self.__tables = self.getRegisterInstance('hiveModel').getDbTables(self.__dbName)

        for curTable in self.__tables:
            dbTable = self.__dbName + '.' + curTable
            self.getRegisterInstance('hiveModel').dropTable(dbTable)

        #self.getRegisterInstance('hiveModel').dropDb(self.__dbName)

