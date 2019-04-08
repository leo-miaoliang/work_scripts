INSERT OVERWRITE table ods.mangguo_webhook_data_callitem_flowinfo PARTITION (etl_date='{{ macros.ds(ti) }}')
SELECT
 `_id`,
 `id`,
 `flow_id`,
 `flow_title`,
 `input`,
 `output`,
 `node_text`,
 `user_label`,
 `record_fpath`,
 `flow_info`
FROM stg.mangguo_webhook_data_callitem_flowinfo
WHERE etl_date='{{ macros.ds(ti) }}'
;
