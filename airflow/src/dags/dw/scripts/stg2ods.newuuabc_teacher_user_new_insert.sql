-- This script was auto generated by dataship on 2019-03-19 11:47 

INSERT OVERWRITE TABLE ods.newuuabc_teacher_user_new
SELECT
  `id` ,
  `uuid` ,
  `email` ,
  `password` ,
  `english_name` ,
  `chinese_name` ,
  `phone` ,
  `other_phone` ,
  `skype` ,
  `birthday` ,
  `age` ,
  `country` ,
  `is_abroad` ,
  `location` ,
  `time_zone` ,
  `salary_type` ,
  `salary_amount` ,
  `salary_live_amount` ,
  `salary_class_amount` ,
  `bank_no` ,
  `bank_name` ,
  `disable` ,
  `icon` ,
  `sex` ,
  `introduce` ,
  `self_introduce` ,
  `comment` ,
  `status` ,
  `passport` ,
  `certificate` ,
  `is_check` ,
  `create_at` ,
  `update_at` ,
  `last_login_ip` ,
  `last_login_at` ,
  `salt` ,
  `emergency_name` ,
  `emergency_phone` ,
  `type` ,
  `culture` ,
  `layer` ,
  `assign_teacher` ,
  `create_id` ,
  `bind_one` ,
  `is_old` ,
  `is_transfer` ,
  `locker` ,
  `tags` ,
  `qualification` ,
  `recommended` ,
  `recommended_info` ,
  load_time
FROM stg.newuuabc_teacher_user_new
WHERE etl_date = '{{ macros.ds(ti) }}'; 


