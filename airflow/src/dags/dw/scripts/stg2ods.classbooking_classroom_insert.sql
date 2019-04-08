-- This script was auto generated by dataship on 2019-03-11 18:25 

DROP TABLE  if exists tmp.tmp_classroom; 
CREATE TABLE tmp.tmp_classroom 
USING PARQUET
AS SELECT
  coalesce(t1.`room_id`, t2.`room_id`) as `room_id`,
  coalesce(t1.`class_date`, t2.`class_date`) as `class_date`,
  coalesce(t1.`status`, t2.`status`) as `status`,
  coalesce(t1.`teacher_id`, t2.`teacher_id`) as `teacher_id`,
  coalesce(t1.`class_type_id`, t2.`class_type_id`) as `class_type_id`,
  coalesce(t1.`start_carport`, t2.`start_carport`) as `start_carport`,
  coalesce(t1.`end_carport`, t2.`end_carport`) as `end_carport`,
  coalesce(t1.`start_time`, t2.`start_time`) as `start_time`,
  coalesce(t1.`end_time`, t2.`end_time`) as `end_time`,
  coalesce(t1.`lesson_category`, t2.`lesson_category`) as `lesson_category`,
  coalesce(t1.`lesson_id`, t2.`lesson_id`) as `lesson_id`,
  coalesce(t1.`train_id`, t2.`train_id`) as `train_id`,
  coalesce(t1.`student_feedback`, t2.`student_feedback`) as `student_feedback`,
  coalesce(t1.`teacher_feedback`, t2.`teacher_feedback`) as `teacher_feedback`,
  coalesce(t1.`teacher_feedback_time`, t2.`teacher_feedback_time`) as `teacher_feedback_time`,
  coalesce(t1.`teacher_entry_time`, t2.`teacher_entry_time`) as `teacher_entry_time`,
  coalesce(t1.`teacher_leave_time`, t2.`teacher_leave_time`) as `teacher_leave_time`,
  coalesce(t1.`video_record`, t2.`video_record`) as `video_record`,
  coalesce(t1.`create_date`, t2.`create_date`) as `create_date`,
  coalesce(t1.`update_date`, t2.`update_date`) as `update_date`,
  coalesce(t1.`operator_id`, t2.`operator_id`) as `operator_id`,
  coalesce(t1.`exception_remark`, t2.`exception_remark`) as `exception_remark`,
  coalesce(t1.`class_property`, t2.`class_property`) as `class_property`,
  coalesce(t1.`class_property_remark`, t2.`class_property_remark`) as `class_property_remark`,
  coalesce(t1.`salary_status`, t2.`salary_status`) as `salary_status`,
  coalesce(t1.`load_time`, t2.`load_time`) as `load_time`
FROM(SELECT
    `room_id`, 
    `class_date`, 
    `status`, 
    `teacher_id`, 
    `class_type_id`, 
    `start_carport`, 
    `end_carport`, 
    `start_time`, 
    `end_time`, 
    `lesson_category`, 
    `lesson_id`, 
    `train_id`, 
    `student_feedback`, 
    `teacher_feedback`, 
    `teacher_feedback_time`, 
    `teacher_entry_time`, 
    `teacher_leave_time`, 
    `video_record`, 
    `create_date`, 
    `update_date`, 
    `operator_id`, 
    `exception_remark`, 
    `class_property`, 
    `class_property_remark`, 
    `salary_status`, 
    `load_time` 
  FROM stg.classbooking_classroom 
  WHERE etl_date = '{{ macros.ds(ti) }}' 
) AS t1
FULL JOIN  
  (SELECT 
    `room_id`, 
    `class_date`, 
    `status`, 
    `teacher_id`, 
    `class_type_id`, 
    `start_carport`, 
    `end_carport`, 
    `start_time`, 
    `end_time`, 
    `lesson_category`, 
    `lesson_id`, 
    `train_id`, 
    `student_feedback`, 
    `teacher_feedback`, 
    `teacher_feedback_time`, 
    `teacher_entry_time`, 
    `teacher_leave_time`, 
    `video_record`, 
    `create_date`, 
    `update_date`, 
    `operator_id`, 
    `exception_remark`, 
    `class_property`, 
    `class_property_remark`, 
    `salary_status`, 
    `load_time`
  FROM ods.classbooking_classroom 
) t2 
  ON t1.`room_id` = t2.`room_id`
;



INSERT OVERWRITE TABLE ods.classbooking_classroom  
SELECT
  `room_id`, 
  `class_date`, 
  `status`, 
  `teacher_id`, 
  `class_type_id`, 
  `start_carport`, 
  `end_carport`, 
  `start_time`, 
  `end_time`, 
  `lesson_category`, 
  `lesson_id`, 
  `train_id`, 
  `student_feedback`, 
  `teacher_feedback`, 
  `teacher_feedback_time`, 
  `teacher_entry_time`, 
  `teacher_leave_time`, 
  `video_record`, 
  `create_date`, 
  `update_date`, 
  `operator_id`, 
  `exception_remark`, 
  `class_property`, 
  `class_property_remark`, 
  `salary_status`, 
  `load_time`
FROM tmp.tmp_classroom; 

