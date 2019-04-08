-- This script was auto generated by dataship on 2018-12-12 10:20 

DROP TABLE  if exists tmp.tmp_contract_template; 
CREATE TABLE tmp.tmp_contract_template 
USING PARQUET
AS SELECT
  coalesce(t1.`id`, t2.`id`) as `id`,
  coalesce(t1.`name`, t2.`name`) as `name`,
  coalesce(t1.`contract_type`, t2.`contract_type`) as `contract_type`,
  coalesce(t1.`deadline`, t2.`deadline`) as `deadline`,
  coalesce(t1.`amount`, t2.`amount`) as `amount`,
  coalesce(t1.`main_class`, t2.`main_class`) as `main_class`,
  coalesce(t1.`add_class`, t2.`add_class`) as `add_class`,
  coalesce(t1.`book`, t2.`book`) as `book`,
  coalesce(t1.`golds`, t2.`golds`) as `golds`,
  coalesce(t1.`leave_count`, t2.`leave_count`) as `leave_count`,
  coalesce(t1.`comment`, t2.`comment`) as `comment`,
  coalesce(t1.`remarks`, t2.`remarks`) as `remarks`,
  coalesce(t1.`status`, t2.`status`) as `status`,
  coalesce(t1.`is_del`, t2.`is_del`) as `is_del`,
  coalesce(t1.`limit_paytime`, t2.`limit_paytime`) as `limit_paytime`,
  coalesce(t1.`limit_paytime_start`, t2.`limit_paytime_start`) as `limit_paytime_start`,
  coalesce(t1.`limit_paytime_end`, t2.`limit_paytime_end`) as `limit_paytime_end`,
  coalesce(t1.`create_at`, t2.`create_at`) as `create_at`,
  coalesce(t1.`update_at`, t2.`update_at`) as `update_at`,
  coalesce(t1.`load_time`, t2.`load_time`) as `load_time`
FROM(SELECT
    `id`, 
    `name`, 
    `contract_type`, 
    `deadline`, 
    `amount`, 
    `main_class`, 
    `add_class`, 
    `book`, 
    `golds`, 
    `leave_count`, 
    `comment`, 
    `remarks`, 
    `status`, 
    `is_del`, 
    `limit_paytime`, 
    `limit_paytime_start`, 
    `limit_paytime_end`, 
    `create_at`, 
    `update_at`, 
    `load_time` 
  FROM stg.newuuabc_contract_template 
  WHERE etl_date = '{{ macros.ds(ti) }}' 
) AS t1
FULL JOIN  
  (SELECT 
    `id`, 
    `name`, 
    `contract_type`, 
    `deadline`, 
    `amount`, 
    `main_class`, 
    `add_class`, 
    `book`, 
    `golds`, 
    `leave_count`, 
    `comment`, 
    `remarks`, 
    `status`, 
    `is_del`, 
    `limit_paytime`, 
    `limit_paytime_start`, 
    `limit_paytime_end`, 
    `create_at`, 
    `update_at`, 
    `load_time`
  FROM ods.newuuabc_contract_template 
) t2 
on t1.`id` = t2.`id`
;



INSERT OVERWRITE TABLE ods.newuuabc_contract_template  
SELECT
  `id`, 
  `name`, 
  `contract_type`, 
  `deadline`, 
  `amount`, 
  `main_class`, 
  `add_class`, 
  `book`, 
  `golds`, 
  `leave_count`, 
  `comment`, 
  `remarks`, 
  `status`, 
  `is_del`, 
  `limit_paytime`, 
  `limit_paytime_start`, 
  `limit_paytime_end`, 
  `create_at`, 
  `update_at`, 
  `load_time`
FROM tmp.tmp_contract_template; 
