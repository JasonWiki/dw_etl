#coding=utf-8
u'''
日期模型对象，所有日期对象都从此类出
'''

from core.util.base.date import Date

class DateModel:

    u'''
    获取昨天日期: 格式，20151029
    '''
    @staticmethod
    def getYesterday():
          timestamp = Date.getOffsetDate(-1)
          return Date.timestampToFormatDate(timestamp,"%Y%m%d")

    u'''
    获取昨天日期: 格式，2015-10-29
    '''
    @staticmethod
    def getYesterdayByYmd():
          timestamp = Date.getOffsetDate(-1)
          return Date.timestampToFormatDate(timestamp,"%Y-%m-%d")

    u'''
    获取今天日期 : 格式，20151030
    '''
    @staticmethod
    def getToday():
        timestamp = Date.getOffsetDate(0)
        return Date.timestampToFormatDate(timestamp,"%Y%m%d")

    u'''
    获取今天日期 : 格式，2015-10-30
    '''
    @staticmethod
    def getTodayByYmd():
        timestamp = Date.getOffsetDate(0)
        return Date.timestampToFormatDate(timestamp,"%Y-%m-%d")

    u'''
    获取明天日期 : 格式，20151031
    '''
    @staticmethod
    def getTomorrow():
        timestamp = Date.getOffsetDate(1)
        return Date.timestampToFormatDate(timestamp,"%Y%m%d")
 
    u'''
    获取明天日期 : 格式，2015-10-31
    '''
    @staticmethod
    def getTomorrowByYmd():
        timestamp = Date.getOffsetDate(1)
        return Date.timestampToFormatDate(timestamp,"%Y-%m-%d")

    u'''
    转换日期格式 : 20151031 -> 2015-10-31
    '''
    @staticmethod
    def dateToFormatYmd(date):
        return Date.dateToFormatDate(date,"%Y%m%d","%Y-%m-%d")
    
    u'''
    获取当前时间
    '''
    @staticmethod
    def getCurrentTime():
        return Date.timestampToFormatDate(Date.getTimestamp(),"%Y-%m-%d %H:%M:%S")

    u'获取当前时间戳'
    @staticmethod
    def getTimestamp():
        return Date.getTimestamp()

    u'根据时间戳，格式化日期'
    @staticmethod
    def timestampToFormatDate(timestamp,dateFormat = "%Y-%m-%d %H:%M:%S"):
        return Date.timestampToFormatDate(timestamp,dateFormat)

    u'''
    获取指定日期偏移天数
    '''
    @staticmethod
    def getOffsetDateDay(date, offsetDay, dateFormat = "%Y%m%d"):
        offsetDate = Date.offsetDateDay(date, offsetDay ,dateFormat)

        return Date.dateToFormatDate(offsetDate,"%Y-%m-%d",dateFormat)
