-- This script was auto generated by dataship on 2019-03-19 11:47 

INSERT OVERWRITE TABLE ods.newuuabc_signed_time
SELECT
  `id` ,
  `teacher_user_id` ,
  `weekday` ,
  `start_time` ,
  `end_time` ,
  `signed_id` ,
  `effective_start_time` ,
  `effective_end_time` ,
  `carport_id` ,
  `create_at` ,
  `update_at` ,
  load_time
FROM stg.newuuabc_signed_time
WHERE etl_date = '{{ macros.ds(ti) }}'; 


