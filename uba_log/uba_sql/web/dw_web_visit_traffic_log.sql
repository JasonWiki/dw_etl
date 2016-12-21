ADD JAR /data/app/jars/dw_hive_udf-1.0-SNAPSHOT-hive.jar;

CREATE TEMPORARY FUNCTION parse_user_agent AS 'com.angejia.dw.hive.udf.useragent.ParseUserAgent';
CREATE TEMPORARY FUNCTION get_page_info AS 'com.angejia.dw.hive.udf.pageinfo.CalculatePageInfo';

-- 插入数据 
INSERT OVERWRITE TABLE dw_db.dw_web_visit_traffic_log PARTITION(p_dt=${dealDate})
SELECT
  -- 用户 ID
  if(length(a.uid)>0,uid,0) AS user_id,
  -- 用户选择城市 ID
  a.ccid as selection_city_id,
  -- 客户端请求时间
  a.client_time AS client_time,
  -- 空字段
  '' AS user_based_city_id,
  -- 上一页 url
  if(length(a.referer)>0,referer,'') AS referer_full_url,
  -- 上一页 path
  coalesce(parse_url(a.referer,'PATH'),'') AS referer_page,
  -- 上一页 page id
  get_page_info(a.referer,'page_id') AS referer_page_id,
  -- 上一页 page name
  get_page_info(a.referer,'page_name') AS referer_page_name,
  -- 当前页 url
  if(length(a.url)>0,url,'') AS current_full_url,
  -- 当前页 path
  coalesce(parse_url(a.url,'PATH'),'') AS current_page,
  -- 当前页 page id
  get_page_info(a.url,'page_id') AS current_page_id,
  -- 当前页 page name
  get_page_info(a.url,'page_name') AS current_page_name,
  -- 当前页 平台编号 1-Touchweb, 2-PC, 3-APP, 4 API
  get_page_info(a.url,'platform_id') AS channel_code,
  -- 页面扩展参数, json 格式
  a.page_param AS page_param,
  -- 请求扩展参数, json 格式
  a.client_param AS client_param,
  -- guid
  a.guid AS guid,
  -- 客户端 ip
  a.ip AS client_ip,
  -- 设备类型
  parse_user_agent(a.agent,0) AS os_type,
  -- 设备版本
  parse_user_agent(a.agent,1) AS os_version,
  -- 浏览器类型
  parse_user_agent(a.agent,2) AS brower_type,
  -- 浏览器版本
  parse_user_agent(a.agent,3) AS brower_version,
  -- 设备客户端类型
  parse_user_agent(a.agent,4) AS phone_type,
  -- 服务器时间
  a.server_time AS server_time,
  -- 上一页 host name
  coalesce(parse_url(a.referer,'HOST'),'') AS referer_host,
  -- 上一页 请求参数
  coalesce(parse_url(a.referer,'QUERY'),'') AS referer_query,
  -- 上一页 锚点
  coalesce(parse_url(a.referer,'REF'),'') AS referer_ref,
  -- 当前页 Host
  coalesce(parse_url(a.url,'HOST'),'') AS current_host,
  -- 当前页 请求参数
  coalesce(parse_url(a.url,'QUERY'),'') AS current_query,
  -- 当前页锚点
  coalesce(parse_url(a.url,'REF'),'') AS current_ref,
  -- host 的城市 id
  coalesce(host_city.city_id,'') AS current_host_city_id

FROM uba_web_visit_log.uba_web_visit_log_${baseDealDate} AS a
-- 通过 host 和 uri 翻译城市 id
LEFT JOIN dim_db.dim_hostname_city AS host_city
  ON (
    CASE
      -- 当为 m.angejia.com 处理下
      WHEN parse_url(a.url,'HOST') = 'm.angejia.com'
        -- 提取城市正则规范
        THEN concat(
          parse_url(a.url,'HOST'),
          regexp_extract( parse_url(a.url,'PATH') ,'^(/[sale|broker]{1,}/[sh|bj|hz|xg]{1,})',1)
        )
      ELSE
        parse_url(a.url,'HOST')
    END
  ) = host_city.hostname
  AND host_city.is_active = 1

-- 过滤 ip
LEFT JOIN dw_db.dw_basis_dimension_filter_ip AS f1
  ON a.ip = f1.client_ip
  AND f1.status = 1

-- 过滤 ip 段
LEFT JOIN dw_db.dw_basis_dimension_filter_ip AS f2
  ON (CONCAT(split(a.ip,'\\.')[0], '.', split(a.ip,'\\.')[1], '.', split(a.ip,'\\.')[2]))
   = (CONCAT(split(f2.client_ip,'\\.')[0], '.', split(f2.client_ip,'\\.')[1], '.', split(f2.client_ip,'\\.')[2]))
  AND f2.status = 1

WHERE
  -- 过滤 agent
  parse_user_agent(a.agent,2) <> 'Robot/Spider'
  AND a.agent NOT LIKE '%spider%'
  AND a.agent NOT LIKE '%-broker%'

-- 过滤 ip 和 ip 段
  AND f1.client_ip IS NULL
  AND f2.client_ip IS NULL
;