
DROP VIEW IF EXISTS tmp_mem_dw_tchops_pref__leave_sys2;
CREATE TEMPORARY VIEW tmp_mem_dw_tchops_pref__leave_sys2 AS
SELECT tun.id AS teacher_id
	, tun.uuid AS teacher_sso_id
	, bui.uid AS teacher_ss_id
	, tun.english_name 
	, '2.0' AS sys
FROM ods.sishu_bk_user_info AS bui
	INNER JOIN ods.sishu_bk_user AS bu
		ON bui.uid = bu.uid
	INNER JOIN ods.newuuabc_teacher_user_new AS tun
		ON bu.uuid = CAST(tun.uuid AS STRING)
WHERE bui.status IN (1, 2)
	AND bui.dpid IN (SELECT id FROM ods.sishu_bk_department WHERE parentid = 24)
	AND tun.status = 3 AND tun.type = 1
;


INSERT overwrite TABLE dw.tchops_pref__leave
-- 2.0
SELECT bl.id as teacher_leave_id
	, t.teacher_id
	, t.teacher_sso_id
	, IF (bl.leave_type IN (1, 2), bl.leave_type, -1) as leave_type -- 1 => 'Sick Leave', 2 => 'Personal Leave'
	, from_unixtime(bl.starttime) AS start_time
	, from_unixtime(bl.endtime) AS end_time
	, bl.notes as comment
	, NULL as create_by
	, from_unixtime(bl.addtime) AS create_time
	, from_unixtime(bl.edittime) AS update_time
	, '2.0' AS sys
	, CURRENT_TIMESTAMP as dw_load_time
FROM ods.sishu_bk_leave AS bl
	INNER JOIN ods.sishu_bk_check AS c
		ON bl.id = c.jnid AND c.`type` = 1
	INNER JOIN tmp_mem_dw_tchops_pref__leave_sys2 AS t
		ON bl.uid = t.teacher_ss_id
WHERE c.sh = 1
	AND bl.display = 1
	AND c.display = 1
UNION ALL
-- 1.0
SELECT tl.id as teacher_leave_id
	, tun.id as teacher_id
	, tun.uuid as teacher_sso_id
	, CASE
		WHEN tl.type = 0 AND tl.start_time < to_unix_timestamp(date '2017-03-01') THEN 2
		WHEN tl.type IN (1, 2, 3) THEN tl.type
		ELSE -1
	  END as leave_type
	, FROM_UNIXTIME(tl.start_time) + INTERVAL '8' HOUR AS start_time
	, FROM_UNIXTIME(tl.end_time) + INTERVAL '8' HOUR AS end_time
	, tl.leave_remark as comment
	, IF(tl.operator_id = 0, NULL, tl.operator_id) as create_by
	, FROM_UNIXTIME(tl.create_at) + INTERVAL '8' HOUR AS create_time
	, FROM_UNIXTIME(tl.update_at) + INTERVAL '8' HOUR AS update_time
	, '1.0' AS sys
	, CURRENT_TIMESTAMP as dw_load_time
FROM ods.newuuabc_teacher_leave AS tl
	INNER JOIN ods.newuuabc_teacher_user_new AS tun
		ON tl.teacher_user_id = tun.id
WHERE tl.status < 3
	AND tun.status = 3 AND tun.type = 1
;

