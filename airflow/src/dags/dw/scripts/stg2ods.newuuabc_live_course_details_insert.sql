-- This script was auto generated by dataship on 2019-03-07 15:22 

INSERT OVERWRITE TABLE ods.newuuabc_live_course_details
SELECT
  `id` ,
  `appoint_course_id` ,
  `flowers` ,
  `golds` ,
  `student_user_id` ,
  `teacher_user_id` ,
  `created` ,
  `diamond` ,
  `forbidden` ,
  `enter_time` ,
  `leave_time` ,
  `correct_number` ,
  `is_review` ,
  `is_evaluate` ,
  `contract_id` ,
  `is_topic` ,
  load_time
FROM stg.newuuabc_live_course_details
WHERE etl_date = '{{ macros.ds(ti) }}'; 


