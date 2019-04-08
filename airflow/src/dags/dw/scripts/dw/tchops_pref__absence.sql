INSERT OVERWRITE TABLE dw.tchops_pref__absence
SELECT ta.id AS teacher_absence_id
	, tun.id AS teacher_id
	, tun.uuid AS teacher_sso_id
	, FROM_UNIXTIME(ta.start_time) AS start_time
	, FROM_UNIXTIME(ta.end_time) AS end_time
	, (ta.end_time - ta.start_time) / 60 AS duration
	, ta.remarks AS comment
	, IF(ta.operator_id = 0, NULL, ta.operator_id) AS create_by
	, FROM_UNIXTIME(ta.create_time) + INTERVAL '8' HOUR as create_time
	, IF(ta.update_at = 0, NULL, FROM_UNIXTIME(ta.update_at) + INTERVAL '8' HOUR) AS update_time
	, '1.0' AS sys
	, CURRENT_TIMESTAMP as dw_load_time
FROM ods.newuuabc_teacher_absenteeism AS ta
	INNER JOIN ods.newuuabc_teacher_user_new AS tun
		ON ta.teacher_id = tun.id
WHERE ta.status < 3 AND tun.status = 3 AND tun.type = 1
;
