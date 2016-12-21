#coding=utf-8
import sys, getopt

class ArgsFormat:

    def __init__(self,args):
        self.setArgs(args)


    def usage(self):
        print '短选项名后的冒号(:)表示该选项必须有附加的参数. 如(-s "uba" -m "init")'
        print '长选项名后的等号(=)表示该选项必须有附加的参数。如(--service "uba" --module "init)'


    # args (入参为:sys.argv)
    args = [];
    def setArgs(self,args):
        self.args = args;
    def getArgs(self):
        return self.args;


    u'''
    设置短选项参数
    入参：
        shortOption STRING (:) 表示需要解析的参数，如："s:m:"
    解释：
        短选项名后的冒号(:)表示该选项必须有附加的参数. 如(-s "uba" -m "init")
    '''
    shortOption = "";
    def setshortOption(self,shortOption):
        self.shortOption = shortOption;
    def getshortOption(self):
        return self.shortOption;


    u'''
    设置长选项参数
    入参：
        longOption 列表[] : 表示需要解析的参数，如：["service=", "mo="]
    解释：
        长选项名后的等号(=)表示该选项必须有附加的参数。如(--service "uba" --mo "init)
    '''
    longOption = [];
    def setlongOption(self,longOption):
        self.longOption = longOption;
    def getlongOption(self):
        return self.longOption;


    #保存处理的结果，转为字典
    result = {};
    def setResult(self,data):
        self.result.update(data);
    def getResult(self):
        return self.result;


    #格式化，解析参数，返回字典 key => val
    def format(self):
        #self.setshortOption("");
        #self.setlongOption(["service=", "mo="])
        try:
            opts, args = getopt.getopt(self.getArgs()[1:],self.getshortOption(),self.getlongOption())
        except getopt.GetoptError, err:
            print str(err)
            self.usage()
            sys.exit(2)

        for op, value in opts:
            self.setResult({op:value});


    def run(self):
        self.format();
        return self.getResult();
