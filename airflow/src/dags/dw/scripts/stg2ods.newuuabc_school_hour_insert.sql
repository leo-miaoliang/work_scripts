-- This script was auto generated by dataship on 2018-12-12 10:21 

INSERT OVERWRITE TABLE ods.newuuabc_school_hour
SELECT
  id ,
  student_id ,
  school_hour ,
  total ,
  used ,
  subject_id ,
  type ,
  update_at ,
  load_time
FROM stg.newuuabc_school_hour
WHERE etl_date = '{{ macros.ds(ti) }}'; 


