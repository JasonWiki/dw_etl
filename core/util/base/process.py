#coding=utf-8
u'''
进程通讯管理
'''

import os
import sys
import subprocess
import threading
import logging
from time import ctime,sleep


u'''
Created on 2015年10月20日

@author: Jason
'''
class Process:


    u'''
    subprocess 模块 fork 后执行脚本，返回进程对象
    @par isWait 是否等待子进程结束
        True : 等待子进程结束后，再退出父进程
        False : 提交子进程结束后，直接退出父进程

    实时获取 shell 结果
    while (True):
            line = p.stdout.readline().strip()
            if (p.poll() == 0):
                break
            if line:
                print line
    '''
    @staticmethod
    def subprocessOpen(command,isWait = True):
        p = subprocess.Popen(
            command,
            shell=True,
            preexec_fn = os.setsid,
            stdin=sys.stdin,
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT)

        # 等待子进程结束
        if (isWait == True) :
            p.wait()

        return p


    u'''
    运行脚本 - 同步模式
    @par command 命令 : 比如 ls /
    @return dict 
        code : 运行状态 0 正常
        stdoutPut : 标准输出
        erroutPut : 标准错误输出(如果错误的话)
    '''
    @staticmethod
    def runScriptSync(command):
        currentP = Process.subprocessOpen(command)

        u'标准输出和标准错误输出'
        stdoutPut,erroutPut = currentP.communicate()

        u'进程退出状态'
        code = currentP.poll()

        result = {
            'code' : code,
            'stdoutPut' : stdoutPut,
            'erroutPut' : erroutPut
        }

        return result


    u'''
    运行脚本 - 异步模式
    return 进程对象：
        currentP.pid : 获取 Pid
        currentP.kill() : 删除进程
        currentP.poll() : 获取结束状态 : 0 表示成功
        currentP.stdout.readline().strip()  : 可通过循环，动态获取输出
    '''
    @staticmethod
    def runScriptAsync(command):
        currentP = Process.subprocessOpen(command,False)

        return currentP


    
    u'''
    多线程执行
    @par commands list :  list = ['ls /','ls ~/']
    @return subprocessOpen 当前提交进程
        后续处理
        status = True
        while (status):
            sleep(1)
            for item in result:
                u'当前进程对象'
                print item
                u'当前进程是否是活动的'
                print item.isAlive()
    '''
    @staticmethod
    def runThreadingScripts(commands):
        u'提交多线程任务'
        for current_command in commands:
             current_thread =  threading.Thread(target=Process.work,args=(current_command,))
             current_thread.setDaemon(True)
             current_thread.start()

        u'当前运行中的Thread对象列表'
        return threading.enumerate()


    @staticmethod
    def work(args):
        p = Process.subprocessOpen(args,True)
        #p = Process.runScriptAsync(args)


    u' ssh 远程执行脚本'
    @staticmethod
    def sshCommand(server_user, server_host, command):
        script = 'ssh -q -t ' + server_user + '@'+ server_host + ' '
        script += '"bash -i '
        script += command
        script += '"'
        return Process.runScriptSync(script)
