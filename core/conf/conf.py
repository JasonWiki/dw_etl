#coding=utf-8
import ConfigParser

from core.core import Core

class Conf:

    cf = None
    def __init__(self,file_name = 'template'):
        if (self.cf == None):
            self.cf = ConfigParser.ConfigParser()

        self.cf.read(Core.SystemPath('confPath') + "/" + file_name +".conf");

    u'''
    获取指定指定类，key 下的数据
    '''
    def getConf(self,category,key):
        return self.cf.get(category, key)

    def setConf(self,category,key,val):
        self.cf.set(category,key,val)

    u'''
    读取配置文件，所有的大类
    '''
    def getConfSections(self):
        return self.cf.sections()
    
    u'''
    获取大类下的所有小组 key
    '''
    def getConfOptions(self,category):
        return self.cf.options(category)

 