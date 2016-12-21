#coding=utf-8
import time
import datetime
import calendar

u'''
Created on 2015年10月20日
@author: Jason
'''
class Date:

    #获取当前时间戳
    @staticmethod
    def getTimestamp():
        return int(time.time())

    u'''
    时间戳转换为日期指定格式日期
    '''
    @staticmethod
    def timestampToFormatDate(timestamp,formatDate = "%Y-%m-%d %H:%M:%S"):
        timeArray = time.localtime(timestamp)
        return time.strftime(formatDate, timeArray)

    u'''
    日期转换为时间戳
    @date String 日期 : 2015-10-20
    @dateFormat String: 日期格式，必须根据 date 的格式一致
    '''
    @staticmethod
    def dateToTimestamp (date,dateFormat = "%Y-%m-%d %H:%M:%S"):
        date = str(date)
        timeArray = time.strptime(date, dateFormat)
        #转换为时间戳:
        timeStamp = int(time.mktime(timeArray))
        return timeStamp

    u'''
    日期直接转换为指定格式的日期
    @date 日期 : 
    @dateFormat 日期格式 : 日期格式，必须根据 date 的格式一致
    @toDateFormat 需要转换成为的日期格式
    '''
    @staticmethod
    def dateToFormatDate(date,dateFormat,toDateFormat):
        date = str(date)
        timeArray = time.strptime(date, dateFormat)
        otherStyleTime = time.strftime(toDateFormat, timeArray)
        return otherStyleTime

    u'''
    获取指定偏移日期
     * @param offset_day 偏移天数，-1昨天 0今天 1明天
     * @return 时间戳
    '''
    @staticmethod
    def getOffsetDate(offset_day = 0):
        daySeconds = 24 * 60 * 60
        curTimestamp = Date.getTimestamp()

        return curTimestamp + (daySeconds * offset_day)


    u'''
    获取指定日期的偏移值
    @param date 指定日期，格式 %Y-%m-%d 2016-04-16
    @param offsetDay 偏移天数，-1昨天 0今天 1明天
    @return 2016-04-16
    '''
    @staticmethod
    def offsetDateDay (date, offsetDay, dateFormat = "%Y-%m-%d"):
        # 当前时间戳
        timestamp = Date.dateToTimestamp(date, dateFormat)
        # 一天的秒数
        daySeconds = 24 * 60 * 60
        # 天数 x 一天的秒数
        seconds = daySeconds * abs(offsetDay)

        rsTimestamp = 0
        if (offsetDay > 0):
            rsTimestamp = timestamp + seconds
        else:
            rsTimestamp = timestamp - seconds

        # 时间戳转换为日期
        return Date.timestampToFormatDate(rsTimestamp, "%Y-%m-%d")