-- signed_time 建模

select  t.teacher_user_id as teacher_id
       ,b.uuid as teacher_sso_id
       , null as teacher_ss_id
       ,b.english_name as teacher_en_name
       ,t.signed_id
       ,case when t.weekday=0 then 7 else t.weekday end as weekday
       ,left(SEC_TO_TIME(start_time*60),5) as start_time
       ,left(SEC_TO_TIME(end_time*60),5) as end_time 
       ,t.start_time as start_time_mins
       ,t.end_time as end_time_mins  
       ,date(FROM_UNIXTIME(t.effective_start_time)) as effective_start_date  
       ,date(FROM_UNIXTIME(t.effective_end_time)) as effective_send_date 
       ,'1.0' as sys
       ,FROM_UNIXTIME(t.create_at+28800) as create_time
       ,FROM_UNIXTIME(t.update_at+28800) as update_time
       ,CURRENT_TIMESTAMP as dw_load_date
from newuuabc.signed_time t
left join newuuabc.teacher_user_new b 
   on t.teacher_user_id=b.id 
union ALL
-- 2.0的
select  t.teacher_id 
       ,b.uuid as teacher_sso_id
       , null as teacher_ss_id
       ,b.english_name as teacher_en_name
       ,t.signed_id
       ,t.weekday
       ,left(SEC_TO_TIME(start_time*60),5) as start_time
       ,left(SEC_TO_TIME(end_time*60),5) as end_time 
       ,t.start_time as start_time_mins
       ,t.end_time as end_time_mins  
       ,date(FROM_UNIXTIME(t.effective_start_time/1000)) as effective_start_date  
       ,date(FROM_UNIXTIME(t.effective_end_time/1000)) as effective_send_date 
       ,'2.0' as sys
       ,FROM_UNIXTIME(t.created_at) as create_time
       ,FROM_UNIXTIME(t.updated_at) as update_time
       ,CURRENT_TIMESTAMP as dw_load_date
from teacher_contract.signed_time t
left join newuuabc.teacher_user_new b 
   on t.teacher_id=b.id 



-- contract 外教签约
select 
     tc.id as contract_id
    ,tc.signed_id
    ,ts.teacher_id
    ,b.uuid as teacher_sso_id
    ,b.english_name as teacher_en_name
    ,tc.salary salary_1v1 
	,tc.salary_live as salary_live
    ,tc.salary_class as salary_class
 	,null as salary_open_class
	,tc.absenteeism as salary_absenteeism
	,tc.wait as salary_wait
	,tc.subsidy as salary_subsidy
	,ts.money_type
	,date(FROM_UNIXTIME(tc.effective_start_time+28800)) as contract_effective_startdate
	,date(FROM_UNIXTIME(tc.effective_end_time+28800)) as contract_effective_enddate
	,date(FROM_UNIXTIME(ts.signed_starttime+28800)) as signed_startdate
	,date(FROM_UNIXTIME(ts.signed_endtime+28800)) as signed_enddate
	,ts.signed_type
	,ts.status as signed_status
	,null as is_contract_enabled
	,ts.status as is_signed_enabled
 	,c.masterid as signed_created_by_id
 	,FROM_UNIXTIME(ts.agree_time+28800) as signed_agree_time
 	,null as level
	,tc.modfiy_time as salary_modify_time
	,FROM_UNIXTIME(ts.created+28800) as signed_create_time
	,FROM_UNIXTIME(ts.updated+28800) as signed_update_time
    ,null contract_create_time
	,null contract_update_time
	,'1.0'	sys
    ,CURRENT_TIMESTAMP dw_load_time
from ods.newuuabc_teacher_contract tc 
left join ods.newuuabc_teacher_signed ts 
   on tc.signed_id=ts.id 
left join ods.newuuabc_teacher_user_new b 
   on ts.teacher_id=b.id 
left join ods.newuuabc_admin c
   on ts.create_user=c.master_name
union all
   -- 2.0 SYSTEM
select
	tc.id as contract_id ,
	tc.signed_id ,
	ts.teacher_id ,
	b.uuid as teacher_sso_id ,
	b.english_name as teacher_en_name ,
	tc.salary salary_1v1 ,
	tc.salary_live as salary_live ,
	tc.salary_class as salary_class ,
	tc.open_course as salary_open_class ,
	tc.absenteeism as salary_absenteeism ,
	tc.wait as salary_wait ,
	tc.subsidy as salary_subsidy ,
	ts.money_type ,
	date(FROM_UNIXTIME(tc.effective_start_time / 1000)) as contract_effective_startdate ,
	date(FROM_UNIXTIME(tc.effective_end_time / 1000)) as contract_effective_enddate ,
	date(FROM_UNIXTIME(ts.signed_starttime / 1000)) as signed_startdate ,
	date(FROM_UNIXTIME(ts.signed_endtime / 1000)) as signed_enddate ,
	ts.signed_type ,
	ts.status as signed_status ,
	tc.enable as is_contract_enabled ,
	ts.status as is_signed_enabled ,
	c.masterid as signed_created_by_id ,
	FROM_UNIXTIME(ts.agree_time) as signed_agree_time ,
	ts.level as level ,
	null as salary_modify_time ,
	FROM_UNIXTIME(ts.created_at) as signed_create_time ,
	FROM_UNIXTIME(ts.updated_at) as signed_update_time ,
	FROM_UNIXTIME(tc.created_at) as contract_create_time ,
	FROM_UNIXTIME(tc.updated_at) as contract_update_time ,
	'2.0' sys ,
	CURRENT_TIMESTAMP dw_load_time
from ods.teacher_contract_teacher_contract tc
left join ods.teacher_contract_teacher_signed ts 
   on tc.signed_id = ts.id
left join ods.newuuabc_teacher_user_new b 
   on ts.teacher_id = b.id
left join ods.newuuabc_admin c 
   on ts.create_user = c.master_name