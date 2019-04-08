
insert overwrite table dw.teaching_lesson_class
-- --------------------------------------------------------------------------------------------------------------------
-- 1v1
select
    t1.id as class_id
    ,case when t1.course_type = 3 then '1v1_formal'
        when t1.course_type = 1 then '1v1_trial'
        when t1.course_type = 2 then '1v1_net_test'
        else 'unknown'
    end as class_type
    ,'1.0' as sys
    ,null as train_id
    -- ,t1.courseware_id
    -- ,t4.courseware_name
    ,t1.teacher_user_id as teacher_id
    ,t7.uuid as teacher_sso_id
    ,case when t1.disabled = 1 and t1.cancel_type <> 2 then 2  -- 2 老师失约，不属于取消课程
        when from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') > '{{ macros.ds(ti) }}' then 1
        else 3
    end as class_status
    -- 1.0# 3学生请假4老师请假5系统障碍6其它 1.5# 4：学生改签
    -- -1.未知 1.学生请假 2.老师请假 3.系统障碍 4.其他
    ,case when t1.disabled = 0 or t1.cancel_type = 2 then null
        when t1.cancel_type in (1, 6) then 4
        when t1.cancel_type = 3 then 1
        when t1.cancel_type = 4 then 2
        when t1.cancel_type = 5 then 3
        else -1
    end as cancel_type    -- 1学生失约2老师失约3学生请假4老师请假5系统障碍6其它
    ,case when t10.last_cancel_time is null or t10.last_cancel_time = 0 then null else from_unixtime(t10.last_cancel_time + 3600 * 8) end as cancel_time
    ,from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') as class_date
    ,round((t1.end_time - t1.start_time) / 60, 0) as duration
    ,case when t1.disabled = 0 or t1.cancel_type = 2 then 1 else 0 end as booked_student_cnt
    ,case when t2.student_into_time is not null and t2.student_into_time > 0 then 1 else 0 end attended_student_cnt
    ,t9.id as timeslot_id
    ,from_unixtime(t1.start_time + 3600 * 8) as start_time
    ,from_unixtime(t1.end_time + 3600 * 8) as end_time
    ,case when from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') > '{{ macros.ds(ti) }}' then null
		when t2.teacher_into_time is not null and t2.teacher_into_time > 0 then 1 
		else 0 
	end as is_teacher_attended  -- 2老师失约
    ,case when t2.teacher_into_time is null or t2.teacher_into_time = 0 then null else from_unixtime(t2.teacher_into_time + 3600 * 8) end as teacher_into_time
    ,case when t2.teacher_out_time is null or t2.teacher_out_time = 0 then null else from_unixtime(t2.teacher_out_time + 3600 * 8) end as teacher_out_time
    ,case when t8.created is null or t8.created = 0 then null else from_unixtime(t8.created + 3600 * 8) end as teacher_feedback_time
    ,case when t1.create_time is null or t1.create_time = 0 then null else from_unixtime(t1.create_time + 3600 * 8) end as create_time
    ,t1.update_time
    ,current_timestamp as dw_load_date
from ods.newuuabc_appoint_course t1
left join
    ods.newuuabc_course_details  t2
on  t1.id = t2.appoint_course_id
and t2.`type` = 1
-- left join
--     newuuabc.courseware t4
-- on  t1.courseware_id = t4.id
left join
    ods.newuuabc_teacher_user_new t7
on  t1.teacher_user_id = t7.id
left join
    ods.newuuabc_teacher_evaluate t8    -- appoint_course_id 存在重复记录
on  t1.id = t8.appoint_course_id
and t8.comment_type in (1, 2)
left join
    dw.dim_timeslot t9
on  from_unixtime(t1.start_time + 3600 * 8, 'HH:mm') = t9.start_time
left join
    (select
        course_id
        ,max(operating_time) as last_cancel_time
    from ods.newuuabc_application_log
    where operating_text in ('取消班级约课成功', '课程取消')
    and `type` = 1
    group by course_id
    )   t10
on  t1.id = t10.course_id
where t1.class_appoint_course_id = 0
-- and date_format(t1.update_time, 'yyyy-MM-dd') = '{{ macros.ds(ti) }}'
-- --------------------------------------------------------------------------------------------------------------------
-- 1v4 1.0
union all
select
    t1.id as class_id
    ,case when t1.course_type = 3 then '1v4_formal'
        when t1.course_type = 1 then '1v4_trial'
        when t1.course_type = 2 then '1v4_net_test'
        when t1.course_type = 5 then 'standby'
        when t1.course_type = 6 then 'realclassroom'
        when t1.course_type = 8 then 'realclassroom_standby'
        else 'unknown'
    end as class_type
    ,'1.0' as sys
    ,null as train_id
    -- ,t1.courseware_id
    -- ,t4.courseware_name
    ,t1.teacher_user_id as teacher_id
    ,t7.uuid as teacher_sso_id
    ,case when t1.disabled = 1 and t1.cancel_type <> 2 then 2  -- 2 老师失约，不属于取消课程
        when from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') > '{{ macros.ds(ti) }}' then 1
        else 3
    end as class_status
    -- 1.0# 3学生请假4老师请假5系统障碍6其它 1.5# 4：学生改签
    -- -1.未知 1.学生请假 2.老师请假 3.系统障碍 4.其他
    ,case when t1.disabled = 0 or t1.cancel_type = 2 then null
        when t1.cancel_type in (1, 6) then 4
        when t1.cancel_type = 3 then 1
        when t1.cancel_type = 4 then 2
        when t1.cancel_type = 5 then 3
        else -1
    end as cancel_type    -- 1学生失约2老师失约3学生请假4老师请假5系统障碍6其它
    ,case when t10.last_cancel_time is null then null else from_unixtime(t10.last_cancel_time + 3600 * 8) end as cancel_time
    ,from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') as class_date
    ,round((t1.end_time - t1.start_time) / 60, 0) as duration
    ,t3.booked_student_cnt
    ,t3.attended_student_cnt
    ,t9.id as timeslot_id
    ,from_unixtime(t1.start_time + 3600 * 8) as start_time
    ,from_unixtime(t1.end_time + 3600 * 8) as end_time
    ,case when from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') > '{{ macros.ds(ti) }}' then null
		when t1.teacher_into_time is not null and t1.teacher_into_time > 0 then 1 
		else 0 
	end as is_teacher_attended  -- 2老师失约
    ,case when t1.teacher_into_time is null or t1.teacher_into_time = 0 then null else from_unixtime(t1.teacher_into_time + 3600 * 8) end as teacher_into_time
    ,case when t1.teacher_out_time is null or t1.teacher_out_time = 0 then null else from_unixtime(t1.teacher_out_time + 3600 * 8) end as teacher_out_time
    ,case when t8.created is null or t8.created = 0 then null else from_unixtime(t8.created + 3600 * 8) end as teacher_feedback_time
    ,case when t1.create_time is null or t1.create_time = 0 then null else from_unixtime(t1.create_time + 3600 * 8) end as create_time
    , t1.update_at as update_time
    , current_timestamp as dw_load_date
from ods.newuuabc_class_appoint_course t1
left join
    (select
        t1.class_appoint_course_id
        ,count(distinct IF(t1.disabled = 0 or (t1.disabled = 1 and t1.cancel_type = 2), t1.id, null)) as booked_student_cnt
		,count(distinct IF(t2.student_into_time is not null and t2.student_into_time > 0, t1.id, null)) as attended_student_cnt
    from ods.newuuabc_appoint_course t1
    left join
        ods.newuuabc_course_details t2
    on  t1.id = t2.appoint_course_id
    and t2.type = 1
    where t1.class_appoint_course_id > 0
    group by t1.class_appoint_course_id
    )   t3
on  t1.id = t3.class_appoint_course_id
-- left join
--     newuuabc.courseware t4
-- on  t1.courseware_id = t4.id
left join
    ods.newuuabc_teacher_user_new t7
on  t1.teacher_user_id = t7.id
left join
    ods.newuuabc_teacher_evaluate t8   -- appoint_course_id 存在重复记录
on  t1.id = t8.appoint_course_id
and t8.comment_type = 3
left join
    dw.dim_timeslot t9
on  from_unixtime(t1.start_time + 3600 * 8, 'HH:mm') = t9.start_time
left join
    (select
        course_id
        ,max(operating_time) as last_cancel_time
    from ods.newuuabc_application_log
    where operating_text in ('取消班级约课成功', '课程取消')
    and `type` = 2
    group by course_id
    )   t10
on  t1.id = t10.course_id
-- where from_unixtime(t1.update_time + 3600 * 8, 'yyyy-MM-dd') = '{{ macros.ds(ti) }}'
-- --------------------------------------------------------------------------------------------------------------------
-- 直播课
union all
select
    t1.id as class_id
    ,'live' as class_type
    ,'1.0' as sys
    ,null as train_id
    ,t1.teacher_user_id as teacher_id
    ,t2.uuid as teacher_sso_id
    ,case when t1.cancel_type > 0 then 2  -- 老师失约不属于取消
        when from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') > '{{ macros.ds(ti) }}' then 1
        else 3
    end as class_status
    -- 1.0# 3学生请假4老师请假5系统障碍6其它 1.5# 4：学生改签
    -- -1.未知 1.学生请假 2.老师请假 3.系统障碍 4.其他
    ,case when t1.attributes = 3 then null   -- 老师旷课不算取消
        WHEN t1.cancel_type = 0 THEN null    -- 未取消
        when t1.cancel_type in (1, 2, 6) then 4
        when t1.cancel_type = 5 then 3
        else -1
    end as cancel_type    -- 1学生失约2老师失约3学生请假4老师请假5系统障碍6其它
    , null as cancel_time
    ,from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') as class_date
    ,t1.class_time as duration
    ,COALESCE(t3.booked_student_cnt, 0) as booked_student_cnt
    ,COALESCE(t3.booked_student_cnt, 0) as attended_student_cnt
    ,t4.id as timeslot_id
    ,from_unixtime(t1.start_time + 3600 * 8) as start_time
    ,from_unixtime(t1.start_time + 60 * t1.class_time + 3600 * 8) as end_time
    ,case when from_unixtime(t1.start_time + 3600 * 8, 'yyyy-MM-dd') > '{{ macros.ds(ti) }}' then null
		when t1.enter_time is not null and t1.enter_time > 0 then 1 
		else 0 
	end as is_teacher_attended
    ,case when t1.enter_time is null or t1.enter_time = 0 then null else from_unixtime(t1.enter_time) end as teacher_into_time
    ,case when t1.leave_time is null or t1.leave_time = 0 then null else from_unixtime(t1.leave_time) end as taecher_out_time
    ,null as teacher_feedback_time
    ,case when t1.create_time is null or t1.create_time = 0 then null else from_unixtime(t1.create_time + 3600 * 8) end as create_time
    ,case when t1.update_time is null or t1.update_time = 0 then null else t1.update_time end as update_time
    ,current_timestamp as dw_load_date
from ods.newuuabc_live_course t1
    left join ods.newuuabc_teacher_user_new t2
        on  t1.teacher_user_id = t2.id
    left join (
        select
            appoint_course_id
            ,count(*) as booked_student_cnt
        from ods.newuuabc_live_course_details
        group by appoint_course_id
    ) t3
        on  t1.id = t3.appoint_course_id
    left join dw.dim_timeslot t4
        on  from_unixtime(t1.start_time + 3600 * 8, 'HH:mm') = t4.start_time
WHERE t1.disabled = 1 -- 仅限已发布的
-- where from_unixtime(t1.update_at + 3600 * 8, 'yyyy-MM-dd') = '{{ macros.ds(ti) }}'
-- --------------------------------------------------------------------------------------------------------------------
-- 1.5
union all
select
    t1.room_id as class_id
    ,case when t1.class_type_id in (1, 5, 6, 7, 8) then '1v4_formal'
        else 'unknown'
    end as class_type
    ,'1.5' as sys
    ,t1.train_id
    -- ,null as courseware_id
    -- ,null as courseware_name
    ,t1.teacher_id
    ,t3.uuid as teacher_sso_id
    ,case when date(from_unixtime(t1.start_time)) > '{{ macros.ds(ti) }}' then 1
        else 3
    end as class_status
    ,null as cancel_type    --
    ,null as cancel_time
    ,from_unixtime(t1.start_time, 'yyyy-MM-dd') as class_date
    ,round((t1.end_time - t1.start_time) / 60, 0) as duration
    ,t2.booked_student_cnt
    ,t2.attended_student_cnt
    ,t4.id as timeslot_id
    ,from_unixtime(t1.start_time) as start_time
    ,from_unixtime(t1.end_time) as end_time
    ,case when date(from_unixtime(t1.start_time)) > '{{ macros.ds(ti) }}' then null
		when t1.teacher_entry_time is not null and t1.teacher_entry_time > 0 then 1 
		else 0 
	end as is_teacher_attended
    ,from_unixtime(t1.teacher_entry_time) as teacher_into_time
    ,from_unixtime(t1.teacher_leave_time) as teacher_out_time
    ,from_unixtime(t1.teacher_feedback_time) as teacher_feedback_time
    ,from_unixtime(t1.create_date) as create_time
    ,from_unixtime(t1.update_date) as update_time
    ,current_timestamp as dw_load_date
from ods.classbooking_classroom t1
left join
    (select
        t1.room_id
        ,sum(case when t1.status in (3, 5, 6, 7, 9) then 1 else 0 end) as booked_student_cnt
        ,sum(case when t2.student_into_time is not null and t2.student_into_time > 0 then 1 else 0 end) as attended_student_cnt
    from ods.classbooking_student_class t1
    left join
        ods.newuuabc_course_details t2
    on  t1.student_class_id = t2.appoint_course_id
    and t2.type = 2
    group by t1.room_id
    )   t2
on  t1.room_id = t2.room_id
left join
    ods.newuuabc_teacher_user_new t3
on  t1.teacher_id = t3.id
left join
    dw.dim_timeslot t4
on  from_unixtime(t1.start_time, 'HH:mm') = t4.start_time
-- where from_unixtime(t1.update_date, 'yyyy-MM-dd') = '{{ macros.ds(ti) }}'
-- --------------------------------------------------------------------------------------------------------------------
-- 2.0
union all
select
    t1.id as class_id
    ,CASE
        WHEN t2.cl_course_type = 3 THEN 'open'
        WHEN t2.fee_common_id = 65 THEN 'standby'
        WHEN t2.cl_typecn IN ('小班课 1V1', '小班课 1V4') THEN '1v4_formal'
        ELSE 'unknown'
    END as class_type
    ,'2.0' as sys
    ,null as train_id
    -- ,t2.courseware_id
    -- ,t3.courseware_name
    ,case when t1.clt_teacher_id = 0 or t1.clt_teacher_id = -1 then null else t1.clt_teacher_id end as teacher_id
    ,t9.uuid as teacher_sso_id
    ,case when from_unixtime(t1.clt_starttime, 'yyyy-MM-dd') > '{{ macros.ds(ti) }}' then 1
        else 3
    end as class_status
    ,null as cancel_type   -- 取值？
    ,null as cancel_time   -- 取值？
    ,from_unixtime(t1.clt_starttime, 'yyyy-MM-dd') as class_date
    ,round((t1.clt_endtime - t1.clt_starttime) / 60, 0) as duration
    ,case
        when t2.cl_course_type = 3 then 0
        WHEN t6.id is null then 0
        else t6.sum_sk + t6.sum_qj + t6.sum_kk + t6.sum_st
      end as booked_student_cnt
    ,case
        when t2.cl_course_type = 3 then 0
        WHEN t6.id is null then 0
        else t6.sum_sk
      end as attended_student_cnt
    ,t8.id as timeslot_id
    ,from_unixtime(t1.clt_starttime) as start_time
    ,from_unixtime(t1.clt_endtime) as end_time
    ,case when from_unixtime(t1.clt_starttime, 'yyyy-MM-dd') > '{{ macros.ds(ti) }}' then null
		when t6.teacher_status in (2, 3) then 0 
		else 1 
	end as is_teacher_attended
    ,t5.teacher_into_time
    ,null as teacher_out_time
    ,null as teacher_feedback_time
    ,t1.create_time as create_time
    ,t1.update_time as update_time
    ,current_timestamp as dw_load_date
from ods.sishu_bk_class_times t1
    left join ods.sishu_bk_class t2
        on  t1.cl_id = t2.id
    left join ods.sishu_bk_user_info t4
        on  t1.clt_teacher_id = t4.id
    left join(
        select
            clt_id
            ,uid
            ,from_unixtime(min(sign_date_unix)) as teacher_into_time
        from ods.sishu_bk_sign_in
        group by clt_id, uid
        ) t5
        on  t1.id = t5.clt_id
            and t4.uid = t5.uid
    left join ods.sishu_bk_subject t6
        on  t1.sbj_id = t6.id
    left join dw.dim_timeslot t8
        on  from_unixtime(t1.clt_starttime, 'HH:mm') = t8.start_time
    left join ods.sishu_bk_user t9
        on  t4.uid = t9.uid
--where date_format(t1.update_time, 'yyyy-MM-dd') = '{{ macros.ds(ti) }}'
;
