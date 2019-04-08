INSERT OVERWRITE table ods.leads_mac_addr PARTITION (etl_date='{{ macros.ds(ti) }}')
SELECT
 `id`     ,
 `mmac`   ,
 `rate`   ,
 `time`   ,
 `lat`    ,
 `lon`    ,
 `mac`    ,
 `rssi`   ,
 `rssi1`  ,
 `rssi2`  ,
 `rssi3`  ,
 `rssi4`  ,
 `ts`     ,
 `tmc`    ,
 `tc`     ,
 `ds`     ,
 `essid0` ,
 `essid1` ,
 `essid2` ,
 `essid3` ,
 `essid4` ,
 `essid5` ,
 `essid6` ,
 `router` ,
 `range`  
FROM stg.leads_mac_addr
WHERE etl_date='{{ macros.ds(ti) }}'
;
