-- 安个家 web 访问日志
CREATE EXTERNAL TABLE IF NOT EXISTS uba_web_visit_log.uba_web_visit_log_${baseDealDate} (
  `uid` string COMMENT 'from deserializer',
  `ccid` string COMMENT 'from deserializer',
  `referer` string COMMENT 'from deserializer',
  `url` string COMMENT 'from deserializer',
  `guid` string COMMENT 'from deserializer',
  `client_time` string COMMENT 'from deserializer',
  `page_param` string COMMENT 'from deserializer',
  `client_param` string COMMENT 'from deserializer',
  `server_time` string COMMENT 'from deserializer',
  `ip` string COMMENT 'from deserializer',
  `agent` string COMMENT 'from deserializer'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde'
STORED AS TEXTFILE
LOCATION '/flume/uba_web_visit/uba_web_visit_${baseDealDate}'
;
