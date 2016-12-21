#coding=utf-8

u'''
处理修改 hive 表的存储格式

适用方法：
    ./index.py --service cluster_task --module hive_tb_store --parameter '' 
'''

from dw_service_core import DwServiceCore

class HiveTbStore(DwServiceCore) :

    def process(self):
        self.hiveTbCompress('db_gather', 'angejia__article_quiz', "p_dt", 'ORC')


    u'获取表字段'
    def getHiveTbFields(self, dbName, tbName, filterFields = []):
        # 获取源数据表字段
        sourceTableFields = self.getRegisterInstance('hiveModel').getFileds(dbName, tbName)
        formatTableFieldsList = []
        for curField in sourceTableFields:
            if (curField in filterFields) : 
                continue
            formatTableFieldsList.append('`' + curField + '`')
        formatSourceTableFieldsStr = ' String,'.join(formatTableFieldsList) + " String"
        return formatSourceTableFieldsStr

    u'''
    hive 表的压缩格式处理
    storedType: 表格式, ORC
    partition: 分区字段
    '''
    def hiveTbCompress(self, dbName, tbName, partition, storedType):
        u'''
        处理思路 
        使用动态分区:
            set hive.exec.dynamic.partition=true;
            set hive.exec.dynamic.partition.mode=nonstrict;
            set hive.exec.max.dynamic.partitions=100000;
            set hive.exec.max.dynamic.partitions.pernode=100000;
        1. 复制原始表到 dw_history_db 中, 做一个备份
            CREATE TABLE dw_history_db.angejia__broker LIKE db_gather.angejia__broker;
            INSERT OVERWRITE TABLE dw_history_db.angejia__broker PARTITION(p_dt) SELECT * FROM db_gather.angejia__broker;
        2. 删除原始表, 并创建新的存储格式的数据表
            DROP TABLE db_gather.angejia__broker;
            CREATE TABLE db_gather.angejia__broker(
              xxx.xxx  这里的字段使用 dw_history_db.angejia__broker 表中的字段
            ) PARTITIONED BY  (
              `p_dt` String
            )
            STORED AS ORC;
        3. 把备份数据写入到新格式的表中
            INSERT OVERWRITE TABLE db_gather.angejia__broker PARTITION(p_dt) SELECT * FROM dw_history_db.angejia__broker;
        '''
        historyDb = "test"

        # 获取表字段
        sourceTbFields = self.getHiveTbFields(dbName, tbName, [partition])

        eltSql = '''
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000; 

set hive.exec.parallel=false;
set mapred.child.java.opts=-Xmx16384M;
set mapreduce.map.java.opts=-Xmx8192M;
set mapreduce.reduce.java.opts=-Xmx16384M;
set mapreduce.map.memory.mb=8192;
set mapreduce.reduce.memory.mb=16384;

-- 复制阶段
CREATE TABLE IF NOT EXISTS %(targetTb)s LIKE %(sourceTb)s;
INSERT OVERWRITE TABLE %(targetTb)s PARTITION(%(partition)s) SELECT * FROM %(sourceTb)s;

-- 删除原始表, 并创建新的存储格式的数据表
DROP TABLE IF EXISTS %(sourceTb)s;
CREATE TABLE %(sourceTb)s(
    %(sourceTbFields)s
) PARTITIONED BY  (
    %(partition)s String
)
STORED AS %(storedType)s;

-- 把数据写入原始表
INSERT OVERWRITE TABLE %(sourceTb)s PARTITION(%(partition)s) SELECT * FROM %(targetTb)s;

'''% {'sourceTb': dbName + '.' + tbName,
       'targetTb': historyDb + '.' + tbName,
       'partition': partition,
       'storedType' : storedType,
       'sourceTbFields' : sourceTbFields
      }

        print eltSql
        #result = self.getRegisterInstance('hiveModel').runHiveScript(eltSql)
        #if (result['code'] == 0) : print result['code']

