ADD JAR /data/app/jars/dw_hive_udf-1.0-SNAPSHOT-hive.jar;

CREATE TEMPORARY FUNCTION parse_action_id_to_page_id AS 'com.angejia.dw.hive.udf.parse.ParseActionIdToPageId';

-- 插入数据
INSERT OVERWRITE TABLE dw_db.dw_app_action_detail_log PARTITION(p_dt = ${dealDate})
SELECT
  -- mac 地址
  a.mac,
  -- 设备 唯一 id
  a.dvid,
  -- 机型
  a.model,
  -- 设备版本
  a.os,
  -- app 名称
  a.name,
  -- 渠道包
  a.ch AS channel,
  -- app 版本号
  a.ver AS version,
  -- user id
  if(length(a.uid)>0,uid,0) AS uid,
  -- 网络类型
  a.net,
  -- ip
  a.ip,
  -- 城市 id
  a.ccid,
  -- 定位城市 id
  a.gcid,
  -- 经纬度
  split(a.geo,'-')[0] AS longtitude,
  split(a.geo,'-')[1] AS latitude,
  -- 动作 id
  a.action AS action_id,
  -- 动作名称
  b.action_name,
  -- 动作英文标号
  b.action_cname,
  -- page id( pageId 匹配模式,  如 actionId 是: 1-5400000 , '最后 000' 表示指定 pageId )
  parse_action_id_to_page_id(a.action) AS currnet_page_id,
  -- page name
  c.action_name AS current_page_name,
  -- page en name
  c.action_cname AS current_page_cname,
  -- 客户端时间
  a.click_time,
  -- 扩展 json 字段
  a.extend,
  -- 上一个 action id
  get_json_object(a.extend,'$.bp') AS bp_id,
  -- 上一个 action name
  d.action_name AS bp_name,
  -- 服务器时间
  a.server_time,
  -- 客户端 Ip
  a.client_ip

-- app 基础表
FROM uba_app_action_log.uba_app_action_log_${baseDealDate} a
-- 解析 actionId
LEFT JOIN dw_db.dw_basis_dimen_action_id_name_lkp b
  ON a.action=b.action_id
  AND b.flag IN (0,3,4)
-- 解析 actionId -> pageId
LEFT JOIN dw_db.dw_basis_dimen_action_id_name_lkp c
  ON parse_action_id_to_page_id(a.action) = c.action_id
  AND c.flag IN (0,3,4)
-- 解析上一级 actionId
LEFT JOIN dw_db.dw_basis_dimen_action_id_name_lkp d
  ON get_json_object(a.extend,'$.bp') = d.action_id

-- 过滤 ip
LEFT JOIN dw_db.dw_basis_dimension_filter_ip AS f1
  ON a.client_ip = f1.client_ip
  AND f1.status = 1

-- 过滤 ip 段
LEFT JOIN dw_db.dw_basis_dimension_filter_ip AS f2
  ON (CONCAT(split(a.client_ip,'\\.')[0], '.', split(a.client_ip,'\\.')[1], '.', split(a.client_ip,'\\.')[2]))
   = (CONCAT(split(f2.client_ip,'\\.')[0], '.', split(f2.client_ip,'\\.')[1], '.', split(f2.client_ip,'\\.')[2]))
  AND f2.status = 1

WHERE
-- 过滤 ip 和 ip 段
  f1.client_ip IS NULL
  AND f2.client_ip IS NULL
;