#!/bin/bash

fn_gmr_regexp+="s/[\n|\r\n]//g;";

# 处理 NULL 字符串

fn_gmr_regexp+="s/NULL/\\\N/g;";
#处理分隔符 
fn_gmr_regexp+="s/\t/$(echo -e ${fields_terminated_by})/g;";
    
#格式化
 mysql "${source_db_type}" "${fn_gmr_mysql_sql}" | sed -e "${fn_gmr_regexp}" > ${now_result_file};"