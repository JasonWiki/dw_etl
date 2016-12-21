#coding=utf-8

u'''
dw 指标体系
'''

from dw_service_core import DwServiceCore

class IndicatorSystem(DwServiceCore) :
    u'''
    获取数据表记录条数
    tableName 数据表
    date 日期 yyyymmdd
    '''
    def getTableCountForDate(self, tableName, date):
        formatDate = self.getRegisterInstance('dateModel').dateToFormatYmd(date)
        querySql = "SELECT COUNT(*) AS c FROM " + tableName + " WHERE p_dt='" + formatDate + "'"

        return self.getRegisterInstance('sparkModel').queryCount(querySql)


    u'''
    获取当前 HDFS MB 已使用的容量
    '''
    def getHdfsMB(self):
        hdfsSize = self.getRegisterInstance('hadoopModel').getHdfsDirSizeForSSH("/")

        dataMb =  hdfsSize.get('dataSize') / 1024 / 1024
        hdfsMb = hdfsSize.get('hdfsSize') / 1024 / 1024

        return {'dataMb' : dataMb, 'hdfsMb' : hdfsMb}


    u'''
    修改指标体系的数据
    date 日期 yyyymmdd
    field 修改的字段
    value 值
    '''
    def modifyIndicatorSystem(self, date, field, value):
        formatDate = self.getRegisterInstance('dateModel').dateToFormatYmd(date)

        # 查询当天数据是否存在
        queryCurDateDateSql = "SELECT id FROM dw_service.indicator_system_sd WHERE p_dt='" + formatDate + "';"
        data = self.getRegisterInstance('biDbModel').queryOne(queryCurDateDateSql)
        
        if (data == None):
            curRunSql = "INSERT INTO dw_service.indicator_system_sd(p_dt," + str(field) + ") values('" + str(formatDate) + "','" + str(value) + "')"
        else:
            curRunSql = "UPDATE dw_service.indicator_system_sd SET " + str(field) + " = '" + str(value) + "' WHERE id = " + str(data.get('id'))

        return self.getRegisterInstance('biDbModel').updataData(curRunSql)