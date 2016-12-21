#coding=utf-8
from core.model.conf_model import ConfModel
from core.util.mysql.mysql_interface import MysqlInterface

u'''
数据部 mysql 服务器模型
'''
class BiDbModel(MysqlInterface):

    def __init__(self):
        systemConf =  ConfModel.getSystemConf()
        dict = {
           'host':systemConf['mysql_bi_db']['host'],
           'port':int(systemConf['mysql_bi_db']['port']),
           'user':systemConf['mysql_bi_db']['user'],
           'passwd':systemConf['mysql_bi_db']['password'],
           'db':'dw_service',
           'charset':'utf8'
        }
        MysqlInterface.__init__(self,dict)


    # 获取数据抽取的数据表信息
    def getExtractMysqlTables(self, extractType):
        sql = "SELECT * FROM dw_service.extract_table WHERE is_delete=0 AND extract_type='" + str(extractType) + "' ORDER BY extract_type ASC,id ASC"
        return self.queryAll(sql)


    # 获取抽取数据表的扩展信息
    def getExtractMysqlTableExt(self, tbId):
        sql = "SELECT * FROM dw_service.extract_table_ext WHERE tb_id = " + str(tbId) + " AND is_del = 0"
        return self.queryOne(sql)


    # 获取 gather 数据表信息
    def getGatherTables(self):
        sql = "SELECT * FROM dw_service.extract_table WHERE is_delete = 0 AND is_gather = 1 ORDER BY id ASC"
        return self.queryAll(sql)


    # 获取 Extract 日志数据
    def getExtractLogForDate(self,date):
        sql = "SELECT * FROM dw_service.extract_log WHERE date_format(created_at,'%Y-%m-%d') = '" + date + "' ORDER BY id DESC;"
        return self.queryAll(sql)