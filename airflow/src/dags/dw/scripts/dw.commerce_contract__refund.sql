DROP TABLE IF EXISTS tmp.tmp_commerce_contract__refund;
CREATE TABLE tmp.tmp_commerce_contract__refund
USING PARQUET
AS
SELECT
	COALESCE(t2.refund_id, t1.refund_id) as refund_id,
	COALESCE(t2.contract_id, t1.contract_id) as contract_id,
	COALESCE(t2.contract_tmpl_id, t1.contract_tmpl_id) as contract_tmpl_id,
	COALESCE(t2.contract_tmpl_name, t1.contract_tmpl_name) as contract_tmpl_name,
	COALESCE(t2.student_id, t1.student_id) as student_id,
	COALESCE(t2.student_sso_id, t1.student_sso_id) as student_sso_id,
	COALESCE(t2.student_name, t1.student_name) as student_name,
	COALESCE(t2.student_phone, t1.student_phone) as student_phone,
	COALESCE(t2.cc_id, t1.cc_id) as cc_id,
	COALESCE(t2.cr_id, t1.cr_id) as cr_id,
	COALESCE(t2.contract_amt, t1.contract_amt) as contract_amt,
	COALESCE(t2.paid_time, t1.paid_time) as paid_time,
	COALESCE(t2.refund_amt, t1.refund_amt) as refund_amt,
	COALESCE(t2.refund_time, t1.refund_time) as refund_time,
	COALESCE(t2.refund_date, t1.refund_date) as refund_date,
	COALESCE(t2.refund_by, t1.refund_by) as refund_by,
	COALESCE(t2.refund_1v1_cnt, t1.refund_1v1_cnt) as refund_1v1_cnt,
	COALESCE(t2.refund_1v4_cnt, t1.refund_1v4_cnt) as refund_1v4_cnt,
	COALESCE(t2.refund_guide_cnt, t1.refund_guide_cnt) as refund_guide_cnt,
	COALESCE(t2.refund_live_cnt, t1.refund_live_cnt) as refund_live_cnt,
	COALESCE(t2.refund_live_replay_cnt, t1.refund_live_replay_cnt) as refund_live_replay_cnt,
	COALESCE(t2.refund_cn_reivew_cnt, t1.refund_cn_reivew_cnt) as refund_cn_reivew_cnt,
	COALESCE(t2.comment, t1.comment) as comment,
	COALESCE(t2.create_by, t1.create_by) as create_by,
	COALESCE(t2.create_time, t1.create_time) as create_time,
	COALESCE(t2.update_time, t1.update_time) as update_time,
	COALESCE(t2.dw_load_time, t1.dw_load_time) as dw_load_time
FROM dw.commerce_contract__refund as t1
FULL JOIN (
	SELECT refund_id
		, contract_id
		, contract_tmpl_id
		, contract_tmpl_name
		, student_id
		, student_sso_id
		, student_name
		, student_phone
		, cc_id
		, cr_id
		, contract_amt
		, paid_time
		, refund_amt
		, refund_time
		, refund_date
		, refund_by
		, SUM(IF(a.subject_id = 1, a.remain + a.free, 0)) as refund_1v1_cnt
		, SUM(IF(a.subject_id = 7, a.remain + a.free, 0)) as refund_1v4_cnt
		, SUM(IF(a.subject_id = 3, a.remain + a.free, 0)) as refund_guide_cnt
		, SUM(IF(a.subject_id = 4, a.remain + a.free, 0)) as refund_live_cnt
		, SUM(IF(a.subject_id = 5, a.remain + a.free, 0)) as refund_live_replay_cnt
		, SUM(IF(a.subject_id = 6, a.remain + a.free, 0)) as refund_cn_reivew_cnt
		, comment
		, create_by
		, create_time
		, update_time
		, CURRENT_TIMESTAMP as dw_load_time
	FROM (
		SELECT cr.id as refund_id
			, c.id as contract_id
			, ct.id as contract_tmpl_id
			, ct.name as contract_tmpl_name
			, su.id as student_id
			, su.uuid as student_sso_id
			, su.name as student_name
			, su.phone as student_phone
			, NULL as cc_id
			, NULL as cr_id
			, c.contract_amount as contract_amt
			, FROM_unixtime(c.sucess_at) + INTERVAL '8' HOUR as paid_time
			, cr.fee as refund_amt
			, FROM_UNIXTIME(cr.refund_time) + INTERVAL '8' HOUR AS refund_time
			, date(FROM_UNIXTIME(cr.refund_time) + INTERVAL '8' HOUR) AS refund_date
			, cr.op_admin_id as refund_by
			, cr.remarks as comment
			, cr.create_admin_id as create_by
			, FROM_UNIXTIME(cr.create_time) as create_time
			, FROM_UNIXTIME(cr.update_time) as update_time
			, cd.subject_id
			, cd.remain
			, cd.free
		FROM stg.newuuabc_contract_refund AS cr
			INNER JOIN ods.newuuabc_contract AS c
				ON cr.contract_id = c.id
			INNER JOIN ods.newuuabc_student_user as su
				ON c.student_id = su.id
			LEFT JOIN ods.newuuabc_contract_template as ct
				ON c.template_id = ct.id
			LEFT JOIN ods.newuuabc_contract_details as cd
				ON cr.contract_id = cd.contract_id
		WHERE cr.status = 1
			AND c.is_del = 1
			AND c.status = 7
			AND c.contract_type <> 3 	-- 非测试合同
			AND (cr.remarks NOT RLIKE '合并|转|换|补|重' or cr.contract_id in(52339, 68826, 20826, 28589, 28606, 53078))
			-- AND cr.remarks NOT LIKE '%合并%' AND cr.remarks NOT LIKE '%转%' AND cr.remarks NOT LIKE '%换%' AND cr.remarks NOT LIKE '%补%' AND cr.remarks NOT LIKE '%重%'
			AND cr.etl_date = '{{ macros.ds(ti) }}'
	) as a
	GROUP BY refund_id
		, contract_id
		, contract_tmpl_id
		, contract_tmpl_name
		, student_id
		, student_sso_id
		, student_name
		, student_phone
		, cc_id
		, cr_id
		, contract_amt
		, paid_time
		, refund_amt
		, refund_time
		, refund_date
		, refund_by
		, comment
		, create_by
		, create_time
		, update_time
)t2 
	ON t1.refund_id = t2.refund_id
;

INSERT OVERWRITE TABLE dw.commerce_contract__refund
SELECT * FROM tmp.tmp_commerce_contract__refund;

