-- 安个家 app 行为 log
CREATE EXTERNAL TABLE IF NOT EXISTS uba_app_action_log.uba_app_action_log_${baseDealDate} (
  `mac` string,
  `dvid` string,
  `model` string,
  `os` string,
  `name` string,
  `ch` string,
  `ver` string,
  `uid` string,
  `net` string,
  `ip` string,
  `ccid` string,
  `gcid` string,
  `geo` string,
  `action` string,
  `click_time` string,
  `extend` string,
  `server_time` string,
  `client_ip` string COMMENT '20150824 add'
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  COLLECTION ITEMS TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/flume/uba_app_action/uba_app_action_${baseDealDate}'
;
