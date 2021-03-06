-- This script was auto generated by dataship on 2019-03-12 10:54 

DROP TABLE  if exists tmp.tmp_contract_details; 
CREATE TABLE tmp.tmp_contract_details 
USING PARQUET
AS SELECT
  coalesce(t1.`id`, t2.`id`) as `id`,
  coalesce(t1.`contract_id`, t2.`contract_id`) as `contract_id`,
  coalesce(t1.`subject_id`, t2.`subject_id`) as `subject_id`,
  coalesce(t1.`total`, t2.`total`) as `total`,
  coalesce(t1.`remain`, t2.`remain`) as `remain`,
  coalesce(t1.`free`, t2.`free`) as `free`,
  coalesce(t1.`free_total`, t2.`free_total`) as `free_total`,
  coalesce(t1.`type`, t2.`type`) as `type`,
  coalesce(t1.`create_time`, t2.`create_time`) as `create_time`,
  coalesce(t1.`update_time`, t2.`update_time`) as `update_time`,
  coalesce(t1.`load_time`, t2.`load_time`) as `load_time`
FROM(SELECT
    `id`, 
    `contract_id`, 
    `subject_id`, 
    `total`, 
    `remain`, 
    `free`, 
    `free_total`, 
    `type`, 
    `create_time`, 
    `update_time`, 
    `load_time` 
  FROM stg.newuuabc_contract_details 
  WHERE etl_date = '{{ macros.ds(ti) }}' 
) AS t1
FULL JOIN  
  (SELECT 
    `id`, 
    `contract_id`, 
    `subject_id`, 
    `total`, 
    `remain`, 
    `free`, 
    `free_total`, 
    `type`, 
    `create_time`, 
    `update_time`, 
    `load_time`
  FROM ods.newuuabc_contract_details 
) t2 
  ON t1.`id` = t2.`id`
;



INSERT OVERWRITE TABLE ods.newuuabc_contract_details  
SELECT
  `id`, 
  `contract_id`, 
  `subject_id`, 
  `total`, 
  `remain`, 
  `free`, 
  `free_total`, 
  `type`, 
  `create_time`, 
  `update_time`, 
  `load_time`
FROM tmp.tmp_contract_details; 

