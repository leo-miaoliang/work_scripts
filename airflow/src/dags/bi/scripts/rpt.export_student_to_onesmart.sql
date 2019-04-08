select
    t1.name
    ,case when t1.sex = 'M' then '男'
        when t1.sex = 'F' then '女'
        else ''
    end as gender
    ,t1.id as stu_id
    ,case when t1.source = 1 then '录入'
        when t1.source = 2 then '官网'
        when t1.source = 3 then '微信'
        when t1.source = 4 then '手机'
        else ''
    end as source
    ,coalesce(t6.phone, '') as phone
    ,coalesce(t1.phone, '') as phone_student
    ,from_unixtime(coalesce(t1.create_time, 0) + 3600 * 8, 'yyyy-MM-dd') as reg_date
    ,coalesce(t7.phone, '') as referrer
    ,coalesce(t6.name, '') as parent_name
    ,coalesce(t1.openid, '') as wechat_openid
    ,case when t1.`type` = 2 then '在读' else '' end as status
    ,coalesce(t1.address, '') as address
    ,coalesce(t1.school, '') as school_name
    ,coalesce(t1.grade, '') as grade
    ,replace(coalesce(t1.birthday, ''), '-', '') as birthday
from ods.newuuabc_student_user t1
left join
    ods.newuuabc_admin t2
on  t1.assign_consultant = t2.masterid
left join
    ods.newuuabc_admin  t3
on  t1.assign_teacher = t3.masterid
left join
    (select
        student_id
        ,max(id) as id
    from ods.newuuabc_parents
    group by student_id
    )   t4
on  t1.id = t4.student_id
left join
    (select distinct
        student_user_id
    from ods.newuuabc_appoint_course
    where course_type = 1
    and disabled = 0
    and status = 3
    )   t5
on  t1.id = t5.student_user_id
left join
    ods.newuuabc_parents t6
on  t4.id = t6.id
left join
    ods.newuuabc_student_user t7
on  t1.recommended = t7.id
where t1.flag = 1
and t1.last_login is not null
and date(t1.update_at) = '{{ macros.ds(ti) }}'
