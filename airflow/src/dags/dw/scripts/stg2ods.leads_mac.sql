INSERT OVERWRITE table ods.leads_mac PARTITION (etl_date='{{ macros.ds(ti) }}')
SELECT
`mac` STRING  ,
`mac_company` ,
`mac_time`    ,
`mac_lat`     ,
`mac_lon`     ,
`geo_hash5`   ,
`geo_hash6`   ,
`geo_hash7`   ,
`geo_hash8`   ,
`geo_hash9`   ,
`geo_category`,
`client_id`   
FROM stg.leads_mac
WHERE etl_date='{{ macros.ds(ti) }}'
;
