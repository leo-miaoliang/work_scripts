
insert overwrite table dw.teaching_lesson_student_class
-- 1.0 1v1
select
    t1.id as student_class_id
    ,t1.id as class_id
    ,case when t1.course_type = 3 then '1v1_formal'
        when t1.course_type = 1 then '1v1_trial'
        when t1.course_type = 2 then 'net_test'
        else 'unknown'
    end as class_type
    ,'1.0' as sys
    ,t1.student_user_id as student_id
    ,t2.uuid as student_sso_id
    ,case when from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') > '{{ macros.ds(ti) }}' then 0
        when t1.disabled = 1 and t1.cancel_type = 3 then 3  --3请假
        when t1.disabled = 1 and t1.cancel_type <> 2 then 4  --4取消
        when t5.student_into_time is not null and t5.student_into_time > 0 then 1 -- 根据时间判断是否出席  1出席
        else 2  -- 2旷课
        -- when t1.cancel_type =
    end as student_class_status
    -- ,case when t1.disabled = 1 and t1.cancel_type not in (2, 3) then 1 else 0 end as cancel_type
	-- 1课程取消 2学生改签 3其他
    ,case when t1.disabled = 1 and t1.cancel_type not in (2, 3) then 1 else null end as cancel_type
    ,null as cancel_time
    ,t1.contract_id
    ,null as deducted_fee
    ,case when t1.contract_id is not null then 1 else null end as deducted_ticket
    ,t4.id as timeslot_id
    ,from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') as class_date
    ,from_unixtime(t1.start_time + 3600 * 8) as start_time
    ,from_unixtime(t1.end_time + 3600 * 8) as end_time
    ,case when t5.student_into_time is null or t5.student_into_time = 0 then null else from_unixtime(t5.student_into_time + 3600 * 8) end as student_into_time
    ,case when t5.student_out_time is null or t5.student_out_time = 0 then null else from_unixtime(t5.student_out_time + 3600 * 8) end as student_out_time
    ,t6.teacher_evaluate as fb_to_teacher_score
    ,t6.evaluate_content as fb_to_teacher_comment
    ,case when t6.created is null or t6.created = 0 then null else from_unixtime(t6.created + 3600 * 8) end as fb_to_teacher_time
    ,current_timestamp as dw_load_date
from ods.newuuabc_appoint_course t1
left join
    ods.newuuabc_student_user t2
on  t1.student_user_id = t2.id
left join
    dw.dim_timeslot t4
on  from_unixtime(t1.start_time + 3600 * 8, 'HH:mm') = t4.start_time
left join
    ods.newuuabc_course_details t5
on  t1.id = t5.appoint_course_id
and t1.student_user_id = t5.student_id
and t5.`type` = 1
left join
    (select
        appoint_course_id
        ,student_user_id
        ,teacher_evaluate
        ,evaluate_content
        ,created
        ,row_number() over(partition by appoint_course_id, student_user_id order by updated desc) as rn
    from ods.newuuabc_student_evaluate
    where `type` = 1
    )   t6
on  t1.id = t6.appoint_course_id
and t1.student_user_id = t6.student_user_id
and t6.rn = 1
where t1.class_appoint_course_id = 0
-- 1.0 1v4
union all
select
    t2.id as student_class_id
    ,t1.id as class_id
    ,case when t1.course_type = 3 then '1v4_formal'
        when t1.course_type = 1 then '1v4_trial'
        when t1.course_type = 2 then 'net_test'
        when t1.course_type = 5 then 'standby'
        when t1.course_type = 6 then 'realclassroom'
        when t1.course_type = 8 then 'realclassroom_standby'
        else 'unknown'
    end as class_type
    ,'1.0' as sys
    ,t2.student_user_id as student_id
    ,t3.uuid as student_sso_id
    ,case when from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') > '{{ macros.ds(ti) }}' then 0
        when t2.disabled = 1 and t2.cancel_type = 3 then 3  --3请假
        when t2.disabled = 1 and t2.cancel_type <> 2 then 4  --4取消
        when t6.student_into_time is not null and t6.student_into_time > 0 then 1 -- 根据时间判断是否出席  1出席
        else 2  -- 2旷课
        -- when t1.cancel_type =
    end as student_class_status
	-- 1课程取消 2学生改签 3其他
    ,case when t2.disabled = 1 and t2.cancel_type not in (2, 3) then 1 else null end as cancel_type
    ,null as cancel_time
    ,t2.contract_id
    ,null as deducted_fee
    ,case when t2.contract_id is not null then 1 else null end as deducted_ticket
    ,t5.id as timeslot_id
    ,from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') as class_date
    ,from_unixtime(t1.start_time + 3600 * 8) as start_time
    ,from_unixtime(t1.end_time + 3600 * 8) as end_time
    ,case when t6.student_into_time is null or t6.student_into_time = 0 then null else from_unixtime(t6.student_into_time + 3600 * 8) end as student_into_time
    ,case when t6.student_out_time is null or t6.student_out_time = 0 then null else from_unixtime(t6.student_out_time + 3600 * 8) end as student_out_time
    ,t7.teacher_evaluate as fb_to_teacher_score
    ,t7.evaluate_content as fb_to_teacher_comment
    ,case when t7.created is null or t7.created = 0 then null else from_unixtime(t7.created + 3600 * 8) end as fb_to_teacher_time
    ,current_timestamp as dw_load_date
from ods.newuuabc_class_appoint_course t1
inner join
    ods.newuuabc_appoint_course t2
on  t1.id = t2.class_appoint_course_id
left join
    ods.newuuabc_student_user t3
on  t2.student_user_id = t3.id
left join
    dw.dim_timeslot t5
on  from_unixtime(t1.start_time + 3600 * 8, 'HH:mm') = t5.start_time
left join
    ods.newuuabc_course_details t6
on  t2.id = t6.appoint_course_id
and t2.student_user_id = t6.student_id
and t6.`type` = 1
left join
    (select
        appoint_course_id
        ,student_user_id
        ,teacher_evaluate
        ,evaluate_content
        ,created
        ,row_number() over(partition by appoint_course_id, student_user_id order by updated desc) as rn
    from ods.newuuabc_student_evaluate
    where `type` = 3
    )   t7
on  t2.id = t7.appoint_course_id
and t2.student_user_id = t7.student_user_id
and t7.rn = 1
-- 1.0 live
union all
select
    t2.id as student_class_id
    ,t1.id as class_id
    ,'live' as class_type
    ,'1.0' as sys
    ,t2.student_user_id as student_id
    ,t3.uuid as student_sso_id
    ,1 as student_class_status
    ,null as cancel_type
    ,null as cancel_time
    ,t2.contract_id
    ,null as deducted_fee
    ,case when t2.contract_id is not null then 1 else null end as deducted_ticket
    ,t5.id as timeslot_id
    ,from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') as class_date
    ,from_unixtime(t1.start_time + 3600 * 8) as start_time
    ,from_unixtime(t1.start_time + 60 * t1.class_time + 3600 * 8) as end_time
    ,case when t2.enter_time is null or t2.enter_time = 0 then null else from_unixtime(t2.enter_time + 3600 * 8) end as student_into_time
    ,case when t2.leave_time is null or t2.leave_time = 0 then null else from_unixtime(t2.leave_time + 3600 * 8) end as student_out_time
    ,t6.teacher_evaluate as fb_to_teacher_score
    ,t6.evaluate_content as fb_to_teacher_comment
    ,case when t6.created is null or t6.created = 0 then null else from_unixtime(t6.created + 3600 * 8) end as fb_to_teacher_time
    ,current_timestamp as dw_load_date
from ods.newuuabc_live_course t1
inner join
    ods.newuuabc_live_course_details t2
on  t1.id = t2.appoint_course_id
left join
    ods.newuuabc_student_user t3
on  t2.student_user_id = t3.id
left join
    dw.dim_timeslot t5
on  from_unixtime(t1.start_time + 3600 * 8, 'HH:mm') = t5.start_time
left join
    (select
        appoint_course_id
        ,student_user_id
        ,teacher_evaluate
        ,evaluate_content
        ,created
        ,row_number() over(partition by appoint_course_id, student_user_id order by updated desc) as rn
    from ods.newuuabc_student_evaluate
    where `type` = 2
    )   t6
on  t2.appoint_course_id = t6.appoint_course_id
and t2.student_user_id = t6.student_user_id
and t6.rn = 1
where from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') > '2017-05-27'
-- 1.5 1v4
union all
select
    t2.student_class_id
    ,t1.room_id as class_id
    ,case when t1.class_type_id in (1, 5, 6, 7, 8) then '1v4_formal'
        else 'unknown'
    end as class_type
    ,'1.5' as sys
    ,t2.student_id
    ,t3.uuid as student_sso_id
    -- 0未上课 1出席 2旷课 3请假 4取消
    ,case when from_unixtime(t1.start_time, 'yyyy-MM-dd') > '{{ macros.ds(ti) }}' then 0
        when t2.student_entry_time is not null and t2.student_entry_time > 0 then 1
        when t2.status = 7 then 3
        when t2.status in (4, 10) then 4
        when t2.student_entry_time is null or t2.student_entry_time = 0 then 2
        else -1
    end as student_class_status
	-- 1课程取消 2学生改签 3其他
    ,case when t2.status = 10 then 1
		when t2.status = 4 then 2
		else null
	end as cancel_type
    ,null as cancel_time
    ,t4.contractId as contract_id
    ,null as deducted_fee
    ,t4.num as deducted_ticket
    ,t5.id as timeslot_id
    ,from_unixtime(t1.start_time, 'yyyy-MM-dd') as class_date
    ,from_unixtime(t1.start_time) as start_time
    ,from_unixtime(t1.end_time) as end_time
    ,from_unixtime(t2.student_entry_time) as student_into_time
    ,case when t7.student_out_time is null or t7.student_out_time = 0 then null else from_unixtime(t7.student_out_time + 3600 * 8, 'yyyy-MM-dd') end as student_out_time
    ,t6.teacher_evaluate as fb_to_teacher_score
    ,t6.evaluate_content as fb_to_teacher_comment
    ,case when t6.created is null or t6.created = 0 then null else from_unixtime(t6.created + 3600 * 8) end as fb_to_teacher_time
    ,current_timestamp as dw_load_date
from ods.classbooking_classroom t1
inner join
    ods.classbooking_student_class t2
on  t1.room_id = t2.room_id
left join
    ods.newuuabc_student_user t3
on  t2.student_id = t3.id
left join
    ods.newuuabc_product_consume t4
on  t2.student_class_id = t4.`key`
and t2.student_id = t4.stuId
left join
    dw.dim_timeslot t5
on  from_unixtime(t1.start_time, 'HH:mm') = t5.start_time
left join
    (select
        appoint_course_id
        ,student_user_id
        ,teacher_evaluate
        ,evaluate_content
        ,created
        ,row_number() over(partition by appoint_course_id, student_user_id order by updated desc) as rn
    from ods.newuuabc_student_evaluate
    where `type` = 4
    )   t6
on  t2.student_class_id = t6.appoint_course_id
and t2.student_id = t6.student_user_id
and t6.rn = 1
left join
	ods.newuuabc_course_details t7
on  t2.student_class_id = t7.appoint_course_id
and t2.student_id = t7.student_id
and t7.`type` = 2
-- 2.0
union all
select
    t4.id as student_class_id
    ,t1.id as class_id
    ,CASE
        WHEN t2.cl_course_type = 3 THEN 'open'
        WHEN t2.fee_common_id = 65 THEN 'standby'
        WHEN t2.cl_typecn IN ('小班课 1V1', '小班课 1V4') THEN '1v4_formal'
        ELSE 'unknown'
    END as class_type
    ,'2.0' as sys
    ,t4.st_id as student_id
    ,t5.uuid as student_sso_id
    ,case when from_unixtime(t1.clt_starttime, 'yyyy-MM-dd') > '{{ macros.ds(ti) }} ' then 0
        when t4.`type` in (1, 4) then 1
        when t4.`type` = 2 then 2
        when t4.`type` = 3 then 3
        else -1
    end as student_class_status
    ,case when t4.`type` = 3 then 1 else null end as cancel_type
    ,null as cancel_time
    ,t4.rectid as contract_id
    ,case when t4.det_ispay = 1 then t4.det_fee_real else null end as deducted_fee
    ,case when t4.det_ispay = 1 then t4.det_time else null end as deducted_ticket
    ,t7.id as timeslot_id
    ,from_unixtime(t1.clt_starttime, 'yyyy-MM-dd') as class_date
    ,from_unixtime(t1.clt_starttime) as start_time
    ,from_unixtime(t1.clt_endtime) as end_time
    ,null as student_into_time
    ,null as student_out_time
    ,t4.teacher_score as fb_to_teacher_score
    ,null as fb_to_teacher_comment
    ,null as fb_to_teacher_time
    ,current_timestamp as dw_load_date
from ods.sishu_bk_class_times t1
left join
    ods.sishu_bk_class t2
on  t1.cl_id = t2.id
-- left join
--     ods.sishu_bk_subject t3
-- on  t1.sbj_id = t3.cl_id
inner join
    ods.sishu_bk_subjectdet t4
on  t1.sbj_id = t4.sbj_id
left join
    ods.sishu_bk_user t5
on  t4.st_id = t5.uid
left join
    dw.dim_timeslot t7
on  from_unixtime(t1.clt_starttime, 'HH:mm') = t7.start_time
;

