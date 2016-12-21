#/bin/bash

# 目录: /home/dwadmin/app/dw_etl
dwServiceHome=$1

# 运行类型: liste   thread
runType=$2

# 运行抽取脚本
$dwServiceHome/index.py --service extract --module extract_run --parameter '{"runType":"'${runType}'"}'
