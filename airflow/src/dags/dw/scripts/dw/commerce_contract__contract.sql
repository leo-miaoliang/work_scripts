DROP TABLE IF EXISTS tmp.tmp_commerce_contract__contract;
CREATE TABLE tmp.tmp_commerce_contract__contract
USING PARQUET
AS
SELECT
	COALESCE(t2.contract_id, t1.contract_id) as contract_id,
	COALESCE(t2.contract_tmpl_id, t1.contract_tmpl_id) as contract_tmpl_id,
	COALESCE(t2.contract_tmpl_name, t1.contract_tmpl_name) as contract_tmpl_name,
	COALESCE(t2.student_id, t1.student_id) as student_id,
	COALESCE(t2.student_sso_id, t1.student_sso_id) as student_sso_id,
	COALESCE(t2.student_name, t1.student_name) as student_name,
	COALESCE(t2.student_phone, t1.student_phone) as student_phone,
	COALESCE(t2.first_cc_id, t1.first_cc_id) as first_cc_id,
	COALESCE(t2.first_cc_name, t1.first_cc_name) as first_cc_name,
	COALESCE(t2.first_cr_id, t1.first_cr_id) as first_cr_id,
	COALESCE(t2.first_cr_name, t1.first_cr_name) as first_cr_name,
	COALESCE(t2.cur_cc_id, t1.cur_cc_id) as cur_cc_id,
	COALESCE(t2.cur_cc_name, t1.cur_cc_name) as cur_cc_name,
	COALESCE(t2.cur_cr_id, t1.cur_cr_id) as cur_cr_id,
	COALESCE(t2.cur_cr_name, t1.cur_cr_name) as cur_cr_name,
	COALESCE(t2.contract_type, t1.contract_type) as contract_type,
	COALESCE(t2.contract_status, t1.contract_status) as contract_status,
	COALESCE(t2.effective_start_date, t1.effective_start_date) as effective_start_date,
	COALESCE(t2.effective_end_date, t1.effective_end_date) as effective_end_date,
	COALESCE(t2.effective_months, t1.effective_months) as effective_months,
	COALESCE(t2.paid_time, t1.paid_time) as paid_time,
	COALESCE(t2.paid_date, t1.paid_date) as paid_date,
	COALESCE(t2.paid_type, t1.paid_type) as paid_type,
	COALESCE(t2.paid_times, t1.paid_times) as paid_times,
	COALESCE(t2.paid_amt, t1.paid_amt) as paid_amt,
	COALESCE(t2.contract_amt, t1.contract_amt) as contract_amt,
	COALESCE(t2.is_renew, t1.is_renew) as is_renew,
	COALESCE(t2.is_refund, t1.is_refund) as is_refund,
	COALESCE(t2.refund_time, t1.refund_time) as refund_time,
	COALESCE(t2.paid_leave_cnt, t1.paid_leave_cnt) as paid_leave_cnt,
	COALESCE(t2.free_leave_cnt, t1.free_leave_cnt) as free_leave_cnt,
	COALESCE(t2.paid_1v1_cnt, t1.paid_1v1_cnt) as paid_1v1_cnt,
	COALESCE(t2.free_1v1_cnt, t1.free_1v1_cnt) as free_1v1_cnt,
	COALESCE(t2.paid_1v4_cnt, t1.paid_1v4_cnt) as paid_1v4_cnt,
	COALESCE(t2.free_1v4_cnt, t1.free_1v4_cnt) as free_1v4_cnt,
	COALESCE(t2.paid_guide_cnt, t1.paid_guide_cnt) as paid_guide_cnt,
	COALESCE(t2.free_guide_cnt, t1.free_guide_cnt) as free_guide_cnt,
	COALESCE(t2.paid_live_cnt, t1.paid_live_cnt) as paid_live_cnt,
	COALESCE(t2.free_live_cnt, t1.free_live_cnt) as free_live_cnt,
	COALESCE(t2.paid_live_replay_cnt, t1.paid_live_replay_cnt) as paid_live_replay_cnt,
	COALESCE(t2.free_live_replay_cnt, t1.free_live_replay_cnt) as free_live_replay_cnt,
	COALESCE(t2.paid_cn_reivew_cnt, t1.paid_cn_reivew_cnt) as paid_cn_reivew_cnt,
	COALESCE(t2.free_cn_reivew_cnt, t1.free_cn_reivew_cnt) as free_cn_reivew_cnt,
	COALESCE(t2.comment, t1.comment) as comment,
	COALESCE(t2.create_by, t1.create_by) as create_by,
	COALESCE(t2.create_time, t1.create_time) as create_time,
	COALESCE(t2.update_time, t1.update_time) as update_time,
	COALESCE(t2.dw_load_time, t1.dw_load_time) as dw_load_time
FROM dw.commerce_contract__contract as t1
FULL JOIN (
	SELECT contract_id
		, contract_tmpl_id
		, contract_tmpl_name
		, student_id
		, student_sso_id
		, student_name
		, student_phone
		, first_cc_id
		, first_cc_name
		, first_cr_id
		, first_cr_name
		, cur_cc_id
		, cur_cc_name
		, cur_cr_id
		, cur_cr_name
		, contract_type
		, contract_status
		, effective_start_date
		, effective_end_date
		, effective_months
		, paid_time
		, paid_date
		, paid_type
		, paid_times
		, paid_amt
		, contract_amt
		, is_renew
		, is_refund
		, refund_time
		, paid_leave_cnt
		, free_leave_cnt
		, SUM(IF(a.subject_id = 1, a.total, 0)) AS paid_1v1_cnt
		, SUM(IF(a.subject_id = 1, a.free_total, 0)) AS free_1v1_cnt
		, SUM(IF(a.subject_id = 7, a.total, 0)) AS paid_1v4_cnt
		, SUM(IF(a.subject_id = 7, a.free_total, 0)) AS free_1v4_cnt
		, SUM(IF(a.subject_id = 3, a.total, 0)) AS paid_guide_cnt
		, SUM(IF(a.subject_id = 3, a.free_total, 0)) AS free_guide_cnt
		, SUM(IF(a.subject_id = 4, a.total, 0)) AS paid_live_cnt
		, SUM(IF(a.subject_id = 4, a.free_total, 0)) AS free_live_cnt
		, SUM(IF(a.subject_id = 5, a.total, 0)) AS paid_live_replay_cnt
		, SUM(IF(a.subject_id = 5, a.free_total, 0)) AS free_live_replay_cnt
		, SUM(IF(a.subject_id = 6, a.total, 0)) AS paid_cn_reivew_cnt
		, SUM(IF(a.subject_id = 6, a.free_total, 0)) AS free_cn_reivew_cnt
		, comment
		, create_by
		, create_time
		, update_time
		, CURRENT_TIMESTAMP as dw_load_time
	FROM (
		SELECT c.id as contract_id
			, ct.id as contract_tmpl_id
			, ct.name as contract_tmpl_name
			, su.id as student_id
			, su.uuid as student_sso_id
			, su.name as student_name
			, su.phone as student_phone
			, NULL as first_cc_id		-- TODO: 补上学生拉链表的 cc
			, NULL as first_cc_name
			, NULL as first_cr_id		-- TODO: 补上学生拉链表的 cr
			, NULL as first_cr_name
			, a1.masterid as cur_cc_id
			, a1.truename as cur_cc_name
			, a2.masterid as cur_cr_id
			, a2.truename as cur_cr_name
			, IF (c.contract_type in (1, 2, 4, 5), c.contract_type, -1) as contract_type
			, IF (c.status BETWEEN 1 AND 9, c.status, -1) as contract_status
			, DATE(FROM_UNIXTIME(start_time) + INTERVAL '8' HOUR) as effective_start_date
			, ADD_MONTHS(FROM_UNIXTIME(start_time) + INTERVAL '8' HOUR, ct.deadline) as effective_end_date
			, ct.deadline as effective_months
			, FROM_unixtime(c.sucess_at) + INTERVAL '8' HOUR as paid_time
			, DATE(FROM_unixtime(c.sucess_at) + INTERVAL '8' HOUR) as paid_date
			, p.paid_type
			, COALESCE(p.paid_times, 0) as paid_times
			, COALESCE(p.paid_amt, 0) as paid_amt
			, c.contract_amount as contract_amt
			, IF(c.`attribute` = 2, 1, 0) AS is_renew
			, IF(c.status = 7, 1, 0) AS is_refund
			, rf.refund_time
			, leave_total as paid_leave_cnt
			, leave_free_total as free_leave_cnt
			, IF(trim(c.comment) = '', NULL, c.comment) as comment
			, c.create_admin_id as create_by
			, FROM_UNIXTIME(c.create_at) + INTERVAL '8' HOUR as create_time
			, FROM_UNIXTIME(c.update_at) + INTERVAL '8' HOUR as update_time
			, cd.subject_id
			, cd.total
			, cd.free_total
		FROM stg.newuuabc_contract AS c
			INNER JOIN ods.newuuabc_student_user as su
				ON c.student_id = su.id
			INNER JOIN ods.newuuabc_admin as a1
				ON su.assign_consultant = a1.masterid
			INNER JOIN ods.newuuabc_admin as a2
				ON su.assign_teacher = a2.masterid
			LEFT JOIN ods.newuuabc_contract_template as ct
				ON c.template_id = ct.id
			LEFT JOIN (
				SELECT cr.contract_id
					, FROM_UNIXTIME(MAX(cr.refund_time)) + INTERVAL '8' HOUR as refund_time 
				FROM ods.newuuabc_contract_refund as cr
				WHERE cr.status = 1
				GROUP BY cr.contract_id
			) as rf
				ON c.status = 7 AND c.id = rf.contract_id
			LEFT JOIN ods.newuuabc_contract_details as cd
				ON c.id = cd.contract_id
			LEFT JOIN (
				SELECT contract_id
					, SUM(total_fee) as paid_amt
					, concat_ws(',', collect_set(pay_type)) as paid_type
					, COUNT(*) as paid_times
				FROM ods.newuuabc_contract_payment
				WHERE is_success = 2 AND total_fee > 0
				GROUP BY contract_id
			) as p
				ON c.id = p.contract_id
		WHERE c.is_del = 1
			AND c.contract_type <> 3 	-- 非测试合同
			AND su.flag = 1
			AND c.etl_date = '{{ macros.ds(ti) }}'
	) as a
	GROUP BY contract_id
		, contract_tmpl_id
		, contract_tmpl_name
		, student_id
		, student_sso_id
		, student_name
		, student_phone
		, first_cc_id
		, first_cc_name
		, first_cr_id
		, first_cr_name
		, cur_cc_id
		, cur_cc_name
		, cur_cr_id
		, cur_cr_name
		, contract_type
		, contract_status
		, effective_start_date
		, effective_end_date
		, effective_months
		, paid_time
		, paid_date
		, paid_type
		, paid_times
		, paid_amt
		, contract_amt
		, is_renew
		, is_refund
		, refund_time
		, paid_leave_cnt
		, free_leave_cnt
		, comment
		, create_by
		, create_time
		, update_time
) t2
	ON t1.contract_id = t2.contract_id
;


INSERT OVERWRITE TABLE dw.commerce_contract__contract
SELECT * FROM tmp.tmp_commerce_contract__contract;

