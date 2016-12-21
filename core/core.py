#coding=utf-8
import sys,os

from util.base.camel import Camel

class Core:

    u'静态环境变量'
    SYSTEM_PATH = {}

    @staticmethod
    def SystemPath(key = None):

        if (len(Core.SYSTEM_PATH) == 0):
            basePath = sys.path[0];
            Core.SYSTEM_PATH.update({
             'basePath':basePath,
             'corePath':basePath + "/core",
             'confPath':basePath + "/core/conf",
             'modelPath':basePath + "/core/model",
             'utilPath':basePath + "/core/util",
             'shellPath':basePath + "/core/shell",
             'tmpPath':basePath + "/tmp",

            });

        if (key == None):
            return Core.SYSTEM_PATH
        else:
            return Core.SYSTEM_PATH[key]


    #获取 配置文件对象接口
    CONF_OBJ = {}
    @staticmethod
    def getConfInterface(fileName = 'coreConf'):
        # 单例
        if (Core.CONF_OBJ.has_key(fileName) == False):
            from conf.conf import Conf
            #默认返回 core 配置
            if (fileName == 'coreConf'):
                Core.CONF_OBJ[fileName] = Conf()
            else:
                Core.CONF_OBJ[fileName] = Conf(fileName)

        return Core.CONF_OBJ[fileName]


    # 获取模型接口对象
    MODEL_OBJ = {}
    @staticmethod
    def getModelInterface(modelName):
        # 单例
        if (Core.MODEL_OBJ.has_key(modelName) == False):
            # 实例化指定模型对象
            str = 'from model.' + Camel.camelToUnderline(modelName) + '_model import ' + modelName + 'Model';
            str += "\n";
            str += 'Core.MODEL_OBJ[modelName] = ' + modelName + 'Model()'
            exec(str)

        return Core.MODEL_OBJ[modelName]
