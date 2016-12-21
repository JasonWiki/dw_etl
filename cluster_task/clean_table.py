#coding=utf-8

u'''
删除无用数据表，根据 dw_service.clean_table 填写的数据，清理无用数据表

使用方法： ./index.py --service cluster_task --module clean_table --parameter '20151123' (需要清理数据表的日期)
'''

from dw_service_core import DwServiceCore

class CleanTable(DwServiceCore) :

    __date = None

    __dwServiceDb = None

    def process(self):

        self.cleanDwTable()


    u' 获取当前操作日期'
    def getDate(self):
        date = self.getParameter()
        if (date == ''):
            date = self.getRegisterInstance('dateModel').getYesterday()

        self.__date = date

        return self.__date


    u' 清理数据表'
    def cleanDwTable(self):
        tableData = self.getTableData()
        for curTb in tableData:
            self.dropTable(curTb['db_name'] + '.' + curTb['tb_name'],curTb['id'])


    u' 获取待操作数据表'
    def getTableData(self):
        queryDataSql = "SELECT * FROM dw_service.clean_table WHERE status = 0 AND date = " + self.getDate()
        return self.getRegisterInstance('biDbModel').queryAll(queryDataSql)


    u' 删除数据表'
    def dropTable(self,db_tb_name,id):

        idDelHive = self.getRegisterInstance('hiveModel').dropTable(db_tb_name)
        idDelMysql = self.getRegisterInstance('biDbModel').dropTable(db_tb_name)

        if (idDelHive == True and idDelMysql == True):
            upStausSql = "UPDATE dw_service.clean_table SET status = 2 WHERE id = " + str(id);
            upStatus = self.getRegisterInstance('biDbModel').updataData(upStausSql)
