-- This script was auto generated by dataship on 2019-03-19 17:39 

INSERT OVERWRITE TABLE ods.sishu_bk_user
SELECT
  `uid` ,
  `uuid` ,
  `utype` ,
  `username` ,
  `nickname` ,
  `email` ,
  `email_audit` ,
  `mobile` ,
  `mobile_audit` ,
  `skype` ,
  `wechat` ,
  `password_raw` ,
  `password` ,
  `pwd_hash` ,
  `reg_time` ,
  `reg_ip` ,
  `last_login_time` ,
  `last_login_ip` ,
  `status` ,
  `avatars` ,
  `robot` ,
  `themes` ,
  `login_count` ,
  `weixin_openid` ,
  `bangdingtime` ,
  `RYtoken` ,
  `af_token` ,
  `af_tokentime` ,
  `device_id` ,
  `device_type` ,
  `identity` ,
  `decription` ,
  `sex` ,
  `language` ,
  `print` ,
  `err_time` ,
  `resume_id` ,
  `create_time` ,
  `update_time` ,
  load_time
FROM stg.sishu_bk_user
WHERE etl_date = '{{ macros.ds(ti) }}'; 


