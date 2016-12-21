#coding=utf-8

u'''
检测 db_sync 数据库下, 根据 dw_service.clean_table 中的表, 检测没有用到 hive 数据表

使用方法： ./index.py --service cluster_task --module clean_db_sync --parameter ''
'''

from dw_service_core import DwServiceCore


class CleanDbSync(DwServiceCore) :

    __date = None

    __dwServiceDb = None


    def process(self):

        self.cleanDbSync()


    u' 清理数据表'
    def cleanDbSync(self):
        tableData = self.getTableData()
        
        for curTb in tableData:
            tbName = curTb['hive_tb_name']

            # 检测项目开发的依赖
            devTaskRs = self.devTask(tbName)

            minireportRs = self.monireport(tbName)

            # 打印没有依赖的 job
            if len(devTaskRs) == 0 and len(minireportRs) == 0:
                print tbName

 


    u' 获取待操作数据表'
    def getTableData(self):
        querySql = """
SELECT
 bs.*
FROM (
  SELECT
    -- 拼接 db_sync
    CONCAT(db_name,'__',tb_name) AS hive_tb_name
  FROM dw_service.extract_table AS bs
  WHERE is_delete=0 AND db_server='product'
) AS bs
;
"""
        return self.getRegisterInstance('biDbModel').queryAll(querySql)


    def devTask(self,tbName):
        querySql = "SELECT * FROM test.dev_task WHERE details like '%" + tbName + "%';"
        return self.getRegisterInstance('biDbModel').queryAll(querySql)

    def monireport(self,tbName):
        querySql = "SELECT * FROM test.mini_report WHERE sp LIKE '%"+tbName+"%';"
        return self.getRegisterInstance('biDbModel').queryAll(querySql)
        
