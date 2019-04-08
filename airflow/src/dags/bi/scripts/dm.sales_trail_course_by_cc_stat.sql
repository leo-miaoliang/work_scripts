
insert overwrite table dm.sales_trail_course_by_cc_stat
select
    t2.cc_id
    ,t2.cc_name
    ,t2.dep_id
    ,t2.dep_name
    ,t2.stat_date
    ,t2.invited_user_cnt
    ,t2.booked_user_cnt
    ,t2.attended_user_cnt
    ,t2.paid_user_cnt
from
    (select
        t1.cc_id
        ,t1.cc_name
        ,t1.dep_id
        ,t1.dep_name
        ,t1.stat_date
        ,sum(t1.invited_user_cnt) as invited_user_cnt
        ,sum(t1.booked_user_cnt) as booked_user_cnt
        ,sum(t1.attended_user_cnt) as attended_user_cnt
        ,sum(t1.paid_user_cnt) as paid_user_cnt
    from
        (SELECT
            a.masterid as cc_id
            ,a.truename as cc_name
            ,d.id as dep_id
            ,d.depart_name as dep_name
            ,from_unixtime(ac.start_time + 3600 * 8, 'yyyy-MM-dd') as stat_date
            ,0 as invited_user_cnt
            ,COUNT(DISTINCT
                case when ac.disabled = 0
                AND ac.course_type = 1
                AND su.flag = 1
                then su.id else null end) AS booked_user_cnt -- `预约试听人数`
            ,COUNT(DISTINCT
                case when ac.disabled = 0
                AND ac.course_type = 1
                AND su.flag = 1
                and ac.status = 3
                then cd.student_id else null end) AS attended_user_cnt  -- `昨日试听出席人数`
            ,0 as paid_user_cnt
        FROM ods.newuuabc_student_user AS su
        LEFT JOIN
            ods.newuuabc_appoint_course AS ac
        ON  ac.student_user_id = su.id
        LEFT JOIN
            ods.newuuabc_course_details AS cd
        ON  cd.appoint_course_id = ac.id
        AND cd.student_id = su.id
        LEFT JOIN
            ods.newuuabc_admin AS a
        ON  su.assign_consultant = a.masterid
        LEFT JOIN
            ods.newuuabc_department d
        ON  a.dept = d.id
        GROUP BY a.masterid, a.truename, d.id, d.depart_name, from_unixtime(ac.start_time + 3600 * 8, 'yyyy-MM-dd')
        -- 付费人数
        union all
        SELECT
            a.masterid as cc_id
            ,a.truename as cc_name
            ,d.id as dep_id
            ,d.depart_name as dep_name
            ,from_unixtime(c.sucess_at + 3600 * 8, 'yyyy-MM-dd') as stat_date
            ,0 as invited_user_cnt
            ,0 as booked_user_cnt
            ,0 as attended_user_cnt
            ,COUNT(DISTINCT
                case when c.contract_type = 1
                AND c.is_del = 1
                AND c.contract_amount > 0
                AND su.flag = 1
                THEN su.id ELSE NULL END) AS paid_user_cnt -- `付费人数`
        FROM ods.newuuabc_student_user AS su
        LEFT JOIN
            ods.newuuabc_appoint_course AS ac
        ON  ac.student_user_id = su.id
        LEFT JOIN
            ods.newuuabc_admin AS a
        ON  su.assign_consultant = a.masterid
        LEFT JOIN
            ods.newuuabc_contract c
        on  su.id = c.student_id
        LEFT JOIN
            ods.newuuabc_department d
        ON  a.dept = d.id
        GROUP BY a.masterid, a.truename, d.id, d.depart_name, from_unixtime(c.sucess_at + 3600 * 8, 'yyyy-MM-dd')
        -- 试听邀约人数
        union all
        SELECT
            a.masterid as cc_id
            ,a.truename as cc_name
            ,d.id as dep_id
            ,d.depart_name as dep_name
            ,from_unixtime(ac.create_time + 3600 * 8, 'yyyy-MM-dd') as stat_date
            ,COUNT(DISTINCT
                case when ac.disabled = 0
                AND ac.course_type = 1
                AND su.flag = 1
                THEN su.id ELSE NULL END) AS invited_user_cnt -- `试听邀约人数`
            ,0 as booked_user_cnt
            ,0 as attended_user_cnt
            ,0 as paid_user_cnt
        FROM ods.newuuabc_student_user AS su
        LEFT JOIN
            ods.newuuabc_appoint_course AS ac
        ON  ac.student_user_id = su.id
        LEFT JOIN
            ods.newuuabc_admin AS a
        ON  su.assign_consultant = a.masterid
        LEFT JOIN
            ods.newuuabc_department d
        ON  a.dept = d.id
        GROUP BY a.masterid, a.truename, d.id, d.depart_name, from_unixtime(ac.create_time + 3600 * 8, 'yyyy-MM-dd')
        )   t1
    group by t1.cc_id, t1.cc_name, t1.dep_id, t1.dep_name, t1.stat_date
    )   t2
where t2.dep_id is not null
and (t2.booked_user_cnt > 0
or  t2.attended_user_cnt > 0
or  t2.paid_user_cnt > 0
or  t2.invited_user_cnt > 0
)
distribute by year(t2.stat_date)
;

