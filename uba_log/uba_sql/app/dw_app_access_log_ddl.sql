-- 创建 app access log
CREATE TABLE if NOT exists dw_db.dw_app_access_log (
   app_name  string,
   app_version  string,
   selection_city_id  string,
   location_city_id  string,
   client_ip  string,
   user_id  string,
   network_type  string,
   platform  string,
   device_type  string,
   os_version  string,
   device_id  string,
   delivery_channels  string,
   channel_name string,
   hostname string,
   request_uri string,
   server_date string,
   server_time string,
   request_page_id  string,
   request_page_name  string,
   longitude  string,
   latitude  string
) partitioned by (p_dt string);
