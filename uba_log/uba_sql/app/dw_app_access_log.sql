ADD JAR /data/app/jars/dw_hive_udf-1.0-SNAPSHOT-hive.jar;

CREATE TEMPORARY FUNCTION parse_mobile_token AS 'com.angejia.dw.hive.udf.parse.ParseMobileToken';
CREATE TEMPORARY FUNCTION get_page_info AS 'com.angejia.dw.hive.udf.pageinfo.CalculatePageInfo';
CREATE TEMPORARY FUNCTION parse_mobile_agent AS 'com.angejia.dw.hive.udf.parse.ParseMobileAgent';

-- 插入数据
INSERT OVERWRITE TABLE dw_db.dw_app_access_log PARTITION(p_dt=${dealDate})
SELECT
  -- app name
  parse_mobile_agent(a.mobile_agent,'app') as app_name,
  parse_mobile_agent(a.mobile_agent,'av') as app_version,
  -- 选择城市
  parse_mobile_agent(a.mobile_agent,'ccid') as selection_city_id,
  -- 本地城市
  parse_mobile_agent(a.mobile_agent,'gcid') as location_city_id,
  -- 客户端 Ip
  remote_addr as client_ip,
  -- 用户 id
  coalesce(parse_mobile_token(auth,'user_id'),0) as user_id,
  -- 网络类型
  parse_mobile_agent(a.mobile_agent,'net') as network_type,
  -- 平台
  parse_mobile_agent(a.mobile_agent,'p') as platform,
  parse_mobile_agent(a.mobile_agent,'pm') as device_type,
  parse_mobile_agent(a.mobile_agent,'osv') as os_version,
  parse_mobile_agent(a.mobile_agent,'dvid') as device_id,
  -- 渠道包号
  parse_mobile_agent(a.mobile_agent,'ch') as delivery_channels,
  -- 渠道包名
  coalesce(c.channel_name,'') as channel_name,
  -- 域名
  hostname as hostname,
  -- 请求 uri
  request_uri as request_uri,
  -- 请求服务器时间
  to_date(server_date) as server_date,
  -- 请求服务器时间
  concat(server_date,' ',server_time) as server_time,
  -- page id
  get_page_info(concat('http://',concat(hostname,request_uri)),'page_id') as request_page_id,
  -- page name
  get_page_info(concat('http://',concat(hostname,request_uri)),'page_name') as request_page_name,
  -- 经纬度
  parse_mobile_agent(a.mobile_agent,'lng') as longitude,
  parse_mobile_agent(a.mobile_agent,'lat') as latitude

FROM access_log.access_log_${baseDealDate} a

-- 过滤 ip
LEFT JOIN dw_db.dw_basis_dimension_filter_ip AS f1
  ON a.remote_addr = f1.client_ip
  AND f1.status = 1

-- 过滤 ip 段
LEFT JOIN dw_db.dw_basis_dimension_filter_ip AS f2
  ON (CONCAT(split(a.remote_addr,'\\.')[0], '.', split(a.remote_addr,'\\.')[1], '.', split(a.remote_addr,'\\.')[2]))
   = (CONCAT(split(f2.client_ip,'\\.')[0], '.', split(f2.client_ip,'\\.')[1], '.', split(f2.client_ip,'\\.')[2]))
  AND f2.status = 1

-- 渠道包
LEFT JOIN dw_db.dw_basis_dimension_delivery_channels_package c
  ON parse_mobile_agent(a.mobile_agent,'ch') = c.channel_package_code

WHERE mobile_agent <> '-'

-- 过滤 ip 和 ip 段
  AND f1.client_ip IS NULL
  AND f2.client_ip IS NULL
;
