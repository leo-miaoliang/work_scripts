-- This script was auto generated by dataship on 2019-03-19 11:47 

INSERT OVERWRITE TABLE ods.sishu_bk_subjectdet
SELECT
  `id` ,
  `sbj_id` ,
  `st_id` ,
  `type` ,
  `sbj_bx` ,
  `sbj_zy` ,
  `sbj_zw` ,
  `sbj_dp` ,
  `sbj_photo` ,
  `homework` ,
  `late` ,
  `ispay` ,
  `sbj_yj` ,
  `op_uid` ,
  `det_fee` ,
  `det_ispay` ,
  `det_time` ,
  `rectid` ,
  `student_fee_id` ,
  `det_fee_real` ,
  `cf_id` ,
  `sf_id` ,
  `file_id` ,
  `diamond` ,
  `teacher_score` ,
  `create_at` ,
  `update_at` ,
  `mark_at` ,
  load_time
FROM stg.sishu_bk_subjectdet
WHERE etl_date = '{{ macros.ds(ti) }}'; 


