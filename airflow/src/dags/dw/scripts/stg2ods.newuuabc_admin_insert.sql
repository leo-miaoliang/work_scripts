-- This script was auto generated by dataship on 2019-03-19 11:47 

INSERT OVERWRITE TABLE ods.newuuabc_admin
SELECT
  `masterid` ,
  `master_name` ,
  `master_password` ,
  `salt` ,
  `english_name` ,
  `truename` ,
  `dept` ,
  `postion` ,
  `create_id` ,
  `purview` ,
  `disable` ,
  `update_time` ,
  `login_ip` ,
  `job_number` ,
  `extension_number` ,
  `main_number` ,
  `icon` ,
  `phone` ,
  `ekt_password` ,
  load_time
FROM stg.newuuabc_admin
WHERE etl_date = '{{ macros.ds(ti) }}'; 


