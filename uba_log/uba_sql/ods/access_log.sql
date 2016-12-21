-- 安个家 app access_log
CREATE EXTERNAL TABLE IF NOT EXISTS access_log.access_log_${baseDealDate} (
  `request_time` string COMMENT '请求时间',
  `upstream_response_time` string COMMENT '响应时间',
  `remote_addr` string COMMENT '请求地址',
  `request_length` string COMMENT '请求大小',
  `upstream_addr` string COMMENT '',
  `server_date` string COMMENT '',
  `server_time` string COMMENT '',
  `hostname` string COMMENT '',
  `method` string COMMENT '',
  `request_uri` string COMMENT '',
  `http_code` string COMMENT '',
  `bytes_sent` string COMMENT '',
  `http_referer` string COMMENT '',
  `user_agent` string COMMENT '',
  `gzip_ratio` string COMMENT '',
  `http_x_forwarded_for` string COMMENT '',
  `auth` string COMMENT '',
  `mobile_agent` string COMMENT '',
  `http_angejia_payload` string COMMENT '',
  `http_trace_id` string COMMENT '',
  `server_protocol` string COMMENT '',
  `ssl_protocol` string COMMENT ''
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  'input.regex'='^([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t\\[(.+?)T(.+?)\\+.*?\\]\\t([^\\t]*)\\t([^\\s]*)\\s([^\\s]*)\\s[^\\t]*\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)',
  'output.format.string'='%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s %10$s %11$s %12$s %13$s %14$s %15$s %16$s %17$s %18$s %19$s %20$s %21$s %22$s'
)
STORED AS TEXTFILE
LOCATION '/flume/access_log/access_log_${baseDealDate}'
;
