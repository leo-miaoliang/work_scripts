-- This script was auto generated by dataship on 2019-03-19 11:47 

INSERT OVERWRITE TABLE ods.newuuabc_appoint_course
SELECT
  `id` ,
  `class_id` ,
  `class_appoint_course_id` ,
  `course_level` ,
  `courseware_id` ,
  `teacher_user_id` ,
  `student_user_id` ,
  `student_words` ,
  `teacher_words` ,
  `start_time` ,
  `end_time` ,
  `course_type` ,
  `subject_id` ,
  `settlement` ,
  `teacher_sum_id` ,
  `settlement_way` ,
  `status` ,
  `remarks` ,
  `cancel_type` ,
  `cancel_reason` ,
  `disabled` ,
  `attributes` ,
  `attributes_remark` ,
  `update_at` ,
  `opt_page` ,
  `direct_id` ,
  `admin_id` ,
  `ssmsession` ,
  `roomtype` ,
  `is_send_message` ,
  `is_wechat_message` ,
  `is_build_report` ,
  `is_first_course` ,
  `contract_id` ,
  `is_free` ,
  `leave_id` ,
  `cancel_is_send_message` ,
  `is_preview` ,
  `is_review` ,
  `is_voice` ,
  `is_evaluate` ,
  `platform` ,
  `is_technical_evaluate` ,
  `create_time` ,
  `replay_upload_status` ,
  `replay_display` ,
  `is_topic` ,
  `update_time` ,
  load_time
FROM stg.newuuabc_appoint_course
WHERE etl_date = '{{ macros.ds(ti) }}'; 

