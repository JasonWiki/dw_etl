#coding=utf-8

u'''
配置对象模型实例，
所有的读取配置，都要通过此类来处理
'''

from core.core import Core

class ConfModel:

    u'''
    私有方法
    组成 Conf 成字典
    '''
    @staticmethod
    def _mergerConfToDict(conf):
        systemInterface = Core.getConfInterface(conf)

        result = {}

        u'获取所有大类的 key '
        sections = systemInterface.getConfSections()

        for curSection in sections:

            u'大类下的小类'
            curOptions = systemInterface.getConfOptions(curSection)

            sectionResult = {}
            for curKey in curOptions :
                sectionResult[curKey] = systemInterface.getConf(curSection,curKey)

            result[curSection] = sectionResult

        return result 

    u'获取 Core 系统环境变量'
    @staticmethod
    def getCoreSystemPath():
        return Core.SystemPath()


    u'系统配置'
    SYSTEM_CONF = None
    @staticmethod
    def getSystemConf():
        if (ConfModel.SYSTEM_CONF == None):
            ConfModel.SYSTEM_CONF = ConfModel._mergerConfToDict('system')

        return ConfModel.SYSTEM_CONF


    u'uba 配置'
    UBA_LOG_CONF = None
    @staticmethod
    def getUbaLogConf():
        if (ConfModel.UBA_LOG_CONF == None):
            ConfModel.UBA_LOG_CONF = ConfModel._mergerConfToDict('uba_log')
        return ConfModel.UBA_LOG_CONF


    u'task 配置'
    TASK_CONF = None
    @staticmethod
    def getTaskConf():
        if (ConfModel.TASK_CONF == None):
            ConfModel.TASK_CONF = ConfModel._mergerConfToDict('task')
        return ConfModel.TASK_CONF


    u'抽取 配置'
    EXTRACT_CONF = None
    @staticmethod
    def getExtractConf():
        if (ConfModel.EXTRACT_CONF == None):
            ConfModel.EXTRACT_CONF = ConfModel._mergerConfToDict('extract')
        return ConfModel.EXTRACT_CONF
