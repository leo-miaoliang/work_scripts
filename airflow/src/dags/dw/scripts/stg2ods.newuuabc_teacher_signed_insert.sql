-- This script was auto generated by dataship on 2019-03-14 14:33 

DROP TABLE  if exists tmp.tmp_newuuabc_teacher_signed; 
CREATE TABLE tmp.tmp_newuuabc_teacher_signed 
USING PARQUET
AS SELECT
  coalesce(t1.`id`, t2.`id`) as `id`,
  coalesce(t1.`teacher_id`, t2.`teacher_id`) as `teacher_id`,
  coalesce(t1.`pid`, t2.`pid`) as `pid`,
  coalesce(t1.`signed_type`, t2.`signed_type`) as `signed_type`,
  coalesce(t1.`signed_starttime`, t2.`signed_starttime`) as `signed_starttime`,
  coalesce(t1.`signed_endtime`, t2.`signed_endtime`) as `signed_endtime`,
  coalesce(t1.`salary`, t2.`salary`) as `salary`,
  coalesce(t1.`absenteeism`, t2.`absenteeism`) as `absenteeism`,
  coalesce(t1.`wait`, t2.`wait`) as `wait`,
  coalesce(t1.`subsidy`, t2.`subsidy`) as `subsidy`,
  coalesce(t1.`status`, t2.`status`) as `status`,
  coalesce(t1.`enable`, t2.`enable`) as `enable`,
  coalesce(t1.`enable_time`, t2.`enable_time`) as `enable_time`,
  coalesce(t1.`create_user`, t2.`create_user`) as `create_user`,
  coalesce(t1.`created`, t2.`created`) as `created`,
  coalesce(t1.`updated`, t2.`updated`) as `updated`,
  coalesce(t1.`effective_start_time`, t2.`effective_start_time`) as `effective_start_time`,
  coalesce(t1.`effective_end_time`, t2.`effective_end_time`) as `effective_end_time`,
  coalesce(t1.`settlement_type`, t2.`settlement_type`) as `settlement_type`,
  coalesce(t1.`money_type`, t2.`money_type`) as `money_type`,
  coalesce(t1.`agree_time`, t2.`agree_time`) as `agree_time`,
  coalesce(t1.`version`, t2.`version`) as `version`,
  coalesce(t1.`load_time`, t2.`load_time`) as `load_time`
FROM(SELECT
    `id`, 
    `teacher_id`, 
    `pid`, 
    `signed_type`, 
    `signed_starttime`, 
    `signed_endtime`, 
    `salary`, 
    `absenteeism`, 
    `wait`, 
    `subsidy`, 
    `status`, 
    `enable`, 
    `enable_time`, 
    `create_user`, 
    `created`, 
    `updated`, 
    `effective_start_time`, 
    `effective_end_time`, 
    `settlement_type`, 
    `money_type`, 
    `agree_time`, 
    `version`, 
    `load_time` 
  FROM stg.newuuabc_teacher_signed 
  WHERE etl_date = '{{ macros.ds(ti) }}' 
) AS t1
FULL JOIN  
  (SELECT 
    `id`, 
    `teacher_id`, 
    `pid`, 
    `signed_type`, 
    `signed_starttime`, 
    `signed_endtime`, 
    `salary`, 
    `absenteeism`, 
    `wait`, 
    `subsidy`, 
    `status`, 
    `enable`, 
    `enable_time`, 
    `create_user`, 
    `created`, 
    `updated`, 
    `effective_start_time`, 
    `effective_end_time`, 
    `settlement_type`, 
    `money_type`, 
    `agree_time`, 
    `version`, 
    `load_time`
  FROM ods.newuuabc_teacher_signed 
) t2 
  ON t1.`id` = t2.`id`
;



INSERT OVERWRITE TABLE ods.newuuabc_teacher_signed  
SELECT
  `id`, 
  `teacher_id`, 
  `pid`, 
  `signed_type`, 
  `signed_starttime`, 
  `signed_endtime`, 
  `salary`, 
  `absenteeism`, 
  `wait`, 
  `subsidy`, 
  `status`, 
  `enable`, 
  `enable_time`, 
  `create_user`, 
  `created`, 
  `updated`, 
  `effective_start_time`, 
  `effective_end_time`, 
  `settlement_type`, 
  `money_type`, 
  `agree_time`, 
  `version`, 
  `load_time`
FROM tmp.tmp_newuuabc_teacher_signed; 
