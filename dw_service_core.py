#coding=utf-8
from core.core import Core as Core
from core.util.base.camel import Camel

import importlib

class DwServiceCore:

    u'获取 Core 实例'
    def getDwCoreInstance(self):
        return Core

    #服务
    service = ""
    def setService(self,data):
        self.service = data

    def getService(self):
        return self.service

    #模块
    module = ""
    def setModule(self,data):
        self.module = data

    def getModule(self):
        return self.module

    # args (入参为:sys.argv)
    args = []
    def setArgs(self,args):
        self.args = args

    def getArgs(self):
        return self.args


    parameter = None;
    def setParameter(self,data = None):
        self.parameter = data;

    def getParameter(self):
        return self.parameter;

    def getFormatParameter(self):
        #print self.getParameter()
        return eval(self.getParameter());


    # 注册实例
    registerInstance = {}
    def setRegisterInstance(self, instanceKey, object):
        self.registerInstance.update({instanceKey : object})

    def getRegisterInstance(self, instanceKey):
        return self.registerInstance.get(instanceKey)


    def init(self):
        # 注册对象

        self.setRegisterInstance('hadoopModel', self.getDwCoreInstance().getModelInterface('Hadoop') )
        self.setRegisterInstance('hiveModel', self.getDwCoreInstance().getModelInterface('Hive') )
        self.setRegisterInstance('sparkModel', self.getDwCoreInstance().getModelInterface('Spark') )
        self.setRegisterInstance('sqoopModel', self.getDwCoreInstance().getModelInterface('Sqoop') )
        self.setRegisterInstance('biDbModel', self.getDwCoreInstance().getModelInterface('BiDb') )
        self.setRegisterInstance('produceDbModel', self.getDwCoreInstance().getModelInterface('ProduceDb') )

        self.setRegisterInstance('mailModel', self.getDwCoreInstance().getModelInterface('Mail') )
        self.setRegisterInstance('dateModel', self.getDwCoreInstance().getModelInterface('Date') )
        self.setRegisterInstance('confModel', self.getDwCoreInstance().getModelInterface('Conf') )


    def process(self):
        pass

    def shutdown(self):
        pass


    def run(self):
        #package = 'test.test_run'
        #module = 'TestRun'
        # 动态加载包和模块
        package = self.getService() + "." + self.getModule()
        module = Camel.underlineToCamel(self.getModule())

        # 加载
        importClass = __import__(package, fromlist=[module])
        className = getattr(importClass, module)

        # 实例
        serviceObject = className()
        serviceObject.setService(self.getService())
        serviceObject.setModule(self.getModule())
        serviceObject.setParameter(self.getParameter())
        serviceObject.setArgs(self.getArgs())
        serviceObject.init()
        serviceObject.process()
        serviceObject.shutdown()


    def runOther(self):
        run_str = u'''
from %s.%s import %s
serviceObj = %s()
serviceObj.setService('%s')
serviceObj.setModule('%s')
serviceObj.setParameter('%s')
serviceObj.setArgs(%s)
serviceObj.init()
serviceObj.process()
serviceObj.shutdown()
'''%(self.getService(),
     self.getModule(),
     Camel.underlineToCamel(self.getModule()),
     Camel.underlineToCamel(self.getModule()),
     self.getService(),
     self.getModule(),
     self.getParameter(),
     self.getArgs()
     )

        exec(run_str);
