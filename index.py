#!/usr/bin/env python
#coding=utf-8
import sys,os

from core.util.base.args_format import ArgsFormat
from dw_service_core import DwServiceCore

argsFormat = ArgsFormat(sys.argv)
#需要解析的长参数
argsFormat.setlongOption(["service=", "module=","parameter="])
#需要解析的段参数
 #argsFormat.setshortOption("m:f:")
argsMap = argsFormat.run()

# 初始化
dwServiceCore = DwServiceCore()
dwServiceCore.setService(argsMap.get('--service'))
dwServiceCore.setModule(argsMap.get('--module'))
dwServiceCore.setParameter(argsMap.get('--parameter'))
dwServiceCore.run()
