#coding=utf-8

from dw_service_core import DwServiceCore

u'''
模板:
 ./index.py --service template --module template_run --parameter '{"date":"20151010"}'
'''

class TemplateRun(DwServiceCore):

    # 初始化
    def init(self):
        #super(DwServiceCore,self).init()
        DwServiceCore.init(self)
        print "init"

    # 处理流程
    def process(self):
        print "process"

    # 关闭
    def shutdown(self):
        print "shutdown"