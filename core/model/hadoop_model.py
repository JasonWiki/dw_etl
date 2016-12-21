#coding=utf-8
from core.model.conf_model import ConfModel
from core.util.base.process import Process

class HadoopModel:

    u'''
    hive 执行脚本命令接口
    '''
    def hadoopCommand(self,command):
        return Process.runScriptSync(command)


    u'获取 hdfs 目录容量'
    def getHdfsDirSize(self, hdfsDir):
        hdfsDir = self.hadoopCommand('hdfs dfs -du '+ hdfsDir)
        result = {}
        if (hdfsDir['code'] == 0):
            strToList =  hdfsDir['stdoutPut'].split('\n')
            filterList = strToList[3:-1]
            for curDir in filterList:
                lineDetail = curDir.split()
                result[lineDetail[2]] = {
                    'fileSize': lineDetail[0],
                    'blockSize':lineDetail[1]
                }
        return result


    u'''
    ssh 连接到 hadoop namenode 服务器远程执行命令
    本机 ~/.ssh/id_rsa.pub 公钥，需要添加 namenode 服务器中
    return 字节
        转为 KB 为 x / 1024
        转为 MB 为 x / 1024 / 1024
        转为 GB 为 x / 1024 / 1024 / 1024
    '''
    def getHdfsDirSizeForSSH(self, hdfsDir): 
        sysConf = ConfModel.getSystemConf()
        nameNodeAccount =  sysConf.get('hadoop').get('hadoop_namenode_account')
        nameNodeHost = sysConf.get('hadoop').get('hadoop_namenode_host')

        command = "hdfs dfs -du -s " + hdfsDir
        commandRs = Process.sshCommand(nameNodeAccount, nameNodeHost, command)

        rs = {}
        if (commandRs.get('code') == 0):
            strToList = commandRs.get('stdoutPut').split('\n')
            # 过滤多余的行
            line = ""
            for curLine in strToList :
                if (len(curLine) == 0):
                    continue
                elif ( "bash" in curLine) :
                    continue
                else :
                    line = curLine
            filterList = line.split()
            rs['dir'] = filterList[2]
            rs['dataSize'] = int(filterList[0])
            rs['hdfsSize'] = int(filterList[1])
        else:
            print commandRs.get('erroutPut')
        return rs
