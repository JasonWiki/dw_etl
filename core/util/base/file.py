#coding=utf-8

import sys,os

u'''
Created on 2015年10月20日

@author: Jason
'''
class File:

    u' 验证文件是否存在'
    @staticmethod
    def isExists(file):
        return os.path.exists(file)


    u'''
    读取文件所有内容
    '''
    @staticmethod
    def redeAll(file):
        f = open(file)
        try:
            allTheText = f.read( )
        finally:
            f.close( )

        return allTheText


    u'''
    写文件

‘r’模式: 以读方式打开，不能进行写操作，文件必须是已经存在的
‘r+’模式：以读写方式打开，文件必须是已经存在的
‘w’模式： 以写方式打开，不能进行读操作，若文件存在，则先清空，然后重新创建；若不存在，则创建文件
‘w+’模式：以读写方式打开，若文件存在，则先清空，然后重新创建；若不存在，则创建文件
‘a’模式： 以追加方式打开，不能进行读操作，把数据追加到文件的末尾；若不存在，则创建文件
‘a+’模式：以读写方式打开，把数据追加到文件的末尾；若不存在，则创建文件
‘b’模式： 以二进制模式打开，不能作为第一个字符出现，需跟以上模式组合使用，如’rb’,’rb+’等，
‘u’模式： 表示通用换行符支持，文件必须是已经存在的
    '''
    @staticmethod
    def write(file, string, model = 'w+'):
        f = open(file, model)
        try:
            f.write(string)
        finally:
            f.close()


    u' 读行'
    @staticmethod
    def readLines(file):
        f = open(file)
        try:
            lineList = f.readlines()
        finally:
            f.close()


    u' 写行'
    @staticmethod
    def writeLines(file, lineList):
        f = open(file,'w')
        try:
            f.writelines(lineList)
        finally:
            f.close()
        