#!/bin/bash
# 使用案例
:<<BLOCK
./sqoop_import_mysql.sh \
--sqoop_home "/usr/local/sqoop" \
--local_tmp_dir "/home/dwadmin/develop/jason/dw_etl/dw_service/tmp" \
--hdfs_sqoop_tmp_dir "/tmp/sqoop" \
--mysql_host "10.10.39.153" \
--mysql_port "3306" \
--mysql_user "angejia_dw" \
--mysql_password "Th872havAyaxEmEB" \
--mysql_database "angejia" \
--mysql_table "article_quiz" \
--hive_table "db_sync.angejia__article_quiz_test" \
--fields_terminated_by "\001" \
--map_column_hive_fields "id=String,article_id=String,inventory_id=String,broker_uid=String,title=String,desc=String,images=String,status=String,operator_uid=String,remark=String,created_at=String,updated_at=String,result_type=String,result_info=String,deleted_at=String,display_type=String,has_set=String" \
--mappers_num "5"
BLOCK

ARGS=`getopt -a -o S -l sqoop_home:,local_tmp_dir:,\
hdfs_sqoop_tmp_dir:,\
mysql_host:,\
mysql_port:,\
mysql_user:,\
mysql_password:,\
mysql_database:,\
mysql_table:,\
hive_table:,\
fields_terminated_by:,\
map_column_hive_fields:,\
mappers_num:,\
help -- "$@"`
[ $? -ne 0 ] && usage  
#set -- "${ARGS}"  
eval set -- "${ARGS}" 

while true  
do  
        case "$1" in 
        -S|--sqoop_home)  
                sqoop_home="$2" 
                shift  
                ;;  
        --local_tmp_dir)  
                local_tmp_dir="$2" 
                shift
                ;;  
        --hdfs_sqoop_tmp_dir)  
                hdfs_sqoop_tmp_dir="$2"
                shift
                ;;
        --mysql_host)  
                mysql_host="$2"
                shift
                ;;
        --mysql_port)  
                mysql_port="$2"
                shift
                ;;
        --mysql_user)  
                mysql_user="$2"
                shift
                ;;
        --mysql_password)  
                mysql_password="$2"
                shift
                ;;
        --mysql_database)  
                mysql_database="$2"
                shift
                ;;
        --mysql_table)  
                mysql_table="$2"
                shift
                ;;
        --hive_table)  
                hive_table="$2"
                shift
                ;;
        --fields_terminated_by)  
                fields_terminated_by="$2"
                shift
                ;;
        --map_column_hive_fields)  
                map_column_hive_fields="$2"
                shift
                ;;
        --mappers_num)  
                mappers_num="$2" 
                shift
                ;;
        --help)  
                usage  
                ;;  
        --)  
                shift  
                break 
                ;;  
        esac  
shift  
done 



sqoop_home=$sqoop_home

# 本地 tmp 目录
local_tmp_dir=$local_tmp_dir

# HDFS SQOOP TMP 目录
hdfs_sqoop_tmp_dir=$hdfs_sqoop_tmp_dir


# MySQL 配置
mysql_host=$mysql_host
mysql_port=$mysql_port
mysql_user=$mysql_user
mysql_password=$mysql_password
mysql_database=$mysql_database
mysql_table=$mysql_table

# hive 数据表
hive_table=$hive_table

# 字段分隔符号
fields_terminated_by=$fields_terminated_by

# hive 指定字段
map_column_hive_fields=$map_column_hive_fields

# Map Reduce 数量
mappers_num=$mappers_num

# sqoop target 表
sqoop_target_dir=${hdfs_sqoop_tmp_dir}/${hive_table}

# sqoop java 输出临时目录
sqoop_outdir=${local_tmp_dir}/sqoop_outdir


# 删除导入数据表时，临时生成的 mysql java
rm -f ${sqoop_outdir}/${mysql_table}.java;

${sqoop_home}/bin/sqoop import \
--connect "jdbc:mysql://${mysql_host}:${mysql_port}/${mysql_database}?useUnicode=true&tinyInt1isBit=false&characterEncoding=utf-8" \
--username "${mysql_user}" \
--password "${mysql_password}" \
--table ${mysql_table} \
--hive-table ${hive_table} \
--hive-import \
--hive-delims-replacement '%n&' \
--fields-terminated-by "${fields_terminated_by}" \
--lines-terminated-by '\n' \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--null-string '\\N' \
--null-non-string '\\N' \
--map-column-hive="${map_column_hive_fields}" \
--outdir ${sqoop_outdir} \
--target-dir ${sqoop_target_dir} \
--delete-target-dir \
--m "${mappers_num}"
