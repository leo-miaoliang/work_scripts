-- This script was auto generated by dataship on 2019-03-07 15:22 

DROP TABLE  if exists tmp.tmp_bk_class; 
CREATE TABLE tmp.tmp_bk_class 
USING PARQUET
AS SELECT
  coalesce(t1.`id`, t2.`id`) as `id`,
  coalesce(t1.`cl_name`, t2.`cl_name`) as `cl_name`,
  coalesce(t1.`cl_type`, t2.`cl_type`) as `cl_type`,
  coalesce(t1.`cl_typecn`, t2.`cl_typecn`) as `cl_typecn`,
  coalesce(t1.`cl_class`, t2.`cl_class`) as `cl_class`,
  coalesce(t1.`cl_classid`, t2.`cl_classid`) as `cl_classid`,
  coalesce(t1.`cl_class_chd`, t2.`cl_class_chd`) as `cl_class_chd`,
  coalesce(t1.`cl_class_chdid`, t2.`cl_class_chdid`) as `cl_class_chdid`,
  coalesce(t1.`cl_subject`, t2.`cl_subject`) as `cl_subject`,
  coalesce(t1.`cl_subjectid`, t2.`cl_subjectid`) as `cl_subjectid`,
  coalesce(t1.`cl_course_type`, t2.`cl_course_type`) as `cl_course_type`,
  coalesce(t1.`cl_course_type_text`, t2.`cl_course_type_text`) as `cl_course_type_text`,
  coalesce(t1.`cl_week`, t2.`cl_week`) as `cl_week`,
  coalesce(t1.`cl_room`, t2.`cl_room`) as `cl_room`,
  coalesce(t1.`cl_timessection`, t2.`cl_timessection`) as `cl_timessection`,
  coalesce(t1.`cl_timestart`, t2.`cl_timestart`) as `cl_timestart`,
  coalesce(t1.`cl_timeend`, t2.`cl_timeend`) as `cl_timeend`,
  coalesce(t1.`cl_datestart`, t2.`cl_datestart`) as `cl_datestart`,
  coalesce(t1.`cl_dateend`, t2.`cl_dateend`) as `cl_dateend`,
  coalesce(t1.`cl_teacherid`, t2.`cl_teacherid`) as `cl_teacherid`,
  coalesce(t1.`cl_teachername`, t2.`cl_teachername`) as `cl_teachername`,
  coalesce(t1.`cl_notes`, t2.`cl_notes`) as `cl_notes`,
  coalesce(t1.`cl_season`, t2.`cl_season`) as `cl_season`,
  coalesce(t1.`cl_seasoncn`, t2.`cl_seasoncn`) as `cl_seasoncn`,
  coalesce(t1.`cl_display`, t2.`cl_display`) as `cl_display`,
  coalesce(t1.`cl_addtime`, t2.`cl_addtime`) as `cl_addtime`,
  coalesce(t1.`cl_edittime`, t2.`cl_edittime`) as `cl_edittime`,
  coalesce(t1.`cl_subschool`, t2.`cl_subschool`) as `cl_subschool`,
  coalesce(t1.`cl_datestop`, t2.`cl_datestop`) as `cl_datestop`,
  coalesce(t1.`cl_is_pay`, t2.`cl_is_pay`) as `cl_is_pay`,
  coalesce(t1.`cl_feeset`, t2.`cl_feeset`) as `cl_feeset`,
  coalesce(t1.`cl_csid`, t2.`cl_csid`) as `cl_csid`,
  coalesce(t1.`cl_numset`, t2.`cl_numset`) as `cl_numset`,
  coalesce(t1.`cl_teacherfee`, t2.`cl_teacherfee`) as `cl_teacherfee`,
  coalesce(t1.`set_fee_type`, t2.`set_fee_type`) as `set_fee_type`,
  coalesce(t1.`fee_common_id`, t2.`fee_common_id`) as `fee_common_id`,
  coalesce(t1.`course_begin_index`, t2.`course_begin_index`) as `course_begin_index`,
  coalesce(t1.`cl_brief_introduction`, t2.`cl_brief_introduction`) as `cl_brief_introduction`,
  coalesce(t1.`cl_ishot`, t2.`cl_ishot`) as `cl_ishot`,
  coalesce(t1.`load_time`, t2.`load_time`) as `load_time`
FROM(SELECT
    `id`, 
    `cl_name`, 
    `cl_type`, 
    `cl_typecn`, 
    `cl_class`, 
    `cl_classid`, 
    `cl_class_chd`, 
    `cl_class_chdid`, 
    `cl_subject`, 
    `cl_subjectid`, 
    `cl_course_type`, 
    `cl_course_type_text`, 
    `cl_week`, 
    `cl_room`, 
    `cl_timessection`, 
    `cl_timestart`, 
    `cl_timeend`, 
    `cl_datestart`, 
    `cl_dateend`, 
    `cl_teacherid`, 
    `cl_teachername`, 
    `cl_notes`, 
    `cl_season`, 
    `cl_seasoncn`, 
    `cl_display`, 
    `cl_addtime`, 
    `cl_edittime`, 
    `cl_subschool`, 
    `cl_datestop`, 
    `cl_is_pay`, 
    `cl_feeset`, 
    `cl_csid`, 
    `cl_numset`, 
    `cl_teacherfee`, 
    `set_fee_type`, 
    `fee_common_id`, 
    `course_begin_index`, 
    `cl_brief_introduction`, 
    `cl_ishot`, 
    `load_time` 
  FROM stg.sishu_bk_class 
  WHERE etl_date = '{{ macros.ds(ti) }}' 
) AS t1
FULL JOIN  
  (SELECT 
    `id`, 
    `cl_name`, 
    `cl_type`, 
    `cl_typecn`, 
    `cl_class`, 
    `cl_classid`, 
    `cl_class_chd`, 
    `cl_class_chdid`, 
    `cl_subject`, 
    `cl_subjectid`, 
    `cl_course_type`, 
    `cl_course_type_text`, 
    `cl_week`, 
    `cl_room`, 
    `cl_timessection`, 
    `cl_timestart`, 
    `cl_timeend`, 
    `cl_datestart`, 
    `cl_dateend`, 
    `cl_teacherid`, 
    `cl_teachername`, 
    `cl_notes`, 
    `cl_season`, 
    `cl_seasoncn`, 
    `cl_display`, 
    `cl_addtime`, 
    `cl_edittime`, 
    `cl_subschool`, 
    `cl_datestop`, 
    `cl_is_pay`, 
    `cl_feeset`, 
    `cl_csid`, 
    `cl_numset`, 
    `cl_teacherfee`, 
    `set_fee_type`, 
    `fee_common_id`, 
    `course_begin_index`, 
    `cl_brief_introduction`, 
    `cl_ishot`, 
    `load_time`
  FROM ods.sishu_bk_class 
) t2 
  ON t1.`id` = t2.`id`
;



INSERT OVERWRITE TABLE ods.sishu_bk_class  
SELECT
  `id`, 
  `cl_name`, 
  `cl_type`, 
  `cl_typecn`, 
  `cl_class`, 
  `cl_classid`, 
  `cl_class_chd`, 
  `cl_class_chdid`, 
  `cl_subject`, 
  `cl_subjectid`, 
  `cl_course_type`, 
  `cl_course_type_text`, 
  `cl_week`, 
  `cl_room`, 
  `cl_timessection`, 
  `cl_timestart`, 
  `cl_timeend`, 
  `cl_datestart`, 
  `cl_dateend`, 
  `cl_teacherid`, 
  `cl_teachername`, 
  `cl_notes`, 
  `cl_season`, 
  `cl_seasoncn`, 
  `cl_display`, 
  `cl_addtime`, 
  `cl_edittime`, 
  `cl_subschool`, 
  `cl_datestop`, 
  `cl_is_pay`, 
  `cl_feeset`, 
  `cl_csid`, 
  `cl_numset`, 
  `cl_teacherfee`, 
  `set_fee_type`, 
  `fee_common_id`, 
  `course_begin_index`, 
  `cl_brief_introduction`, 
  `cl_ishot`, 
  `load_time`
FROM tmp.tmp_bk_class; 

