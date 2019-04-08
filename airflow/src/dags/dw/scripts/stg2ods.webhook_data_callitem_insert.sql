INSERT OVERWRITE table ods.mangguo_webhook_data_callitem PARTITION (etl_date='{{ macros.ds(ti) }}')
SELECT
 `_id`,
 `record_fpath`,
 `who`,
 `text`,
 `create_at`,
 `user_info`,
 `flow_info`
FROM stg.mangguo_webhook_data_callitem
WHERE etl_date='{{ macros.ds(ti) }}'
;
