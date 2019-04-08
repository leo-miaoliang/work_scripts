CREATE TABLE dw.tchops_contract__signed_detail 
         (contract_id BIGINT COMMENT '合同id',
		signed_id BIGINT COMMENT '签约合同id,是contract的子集',
		teacher_id BIGINT COMMENT '老师id',
          teacher_sso_id  STRING comment '单点登录id',
          teacher_ss_id   STRING comment '老师私塾id',
		teacher_en_name STRING comment '英文名字',
		salary_1v1 DOUBLE comment '一对一时薪，钱单位分，来源teacher_contract',
		salary_live DOUBLE comment '直播课时薪，钱单位分，来源teacher_contract',
		salary_class DOUBLE comment '小班课时薪，钱单位分，来源teacher_contract',
		salary_open_class DOUBLE '公开课时薪，钱单位分，来源teacher_contract',
		salary_absenteeism DOUBLE comment '旷工罚款，钱单位分，来源teacher_contract',
		salary_wait DOUBLE comment '等待时薪，钱单位分，来源teacher_contract',
		salary_subsidy DOUBLE comment '每笔工资结算定额补贴，钱单位分，来源teacher_contract',
          money_type BIGINT COMMENT '0: 美元   1：人名币',
		contract_effective_startdate STRING comment '合同生效开始时间，格式YY-MM-DD',
		contract_effective_enddate STRING comment '合同生效结束时间，格式YY-MM-DD',
		signed_startdate STRING comment '分段合同生效开始时间，格式YY-MM-DD',
		signed_enddate STRING comment '分段合同生效结束时间，格式YY-MM-DD',
		signed_type BIGINT COMMENT '1 试用老师；2 兼职签约老师 ；3 全职签约老师',
		signed_status BIGINT COMMENT '签约状态  0 未接受 1 已接受 2 拒绝',
		is_contract_enabled BIGINT COMMENT '合同状态： 1 启用 2 禁用',
          is_signed_enabled BIGINT COMMENT '合同状态：0 未启用 1 启用 2 禁用',
		signed_created_by_id BIGINT COMMENT '合同创建者id',
          signed_agree_time string comment '老师接受协议时间',
		level   STRING comment'2.0新增，1.0置位null',
		salary_modify_time STRING comment '薪资修改时间',
		signed_create_time STRING comment '签约创建时间',
		signed_update_time STRING comment '签约更新时间',
          contract_create_time STRING comment '签约创建时间',
		contract_update_time STRING comment '签约更新时间',
		sys  STRING comment  '1.0,2.0',
          dw_load_time   string comment '加载时间'
          ) comment '外教合同签约信息';

create table dw.tchops_contract__signed_time_his
     (
          teacher_id   bigint comment '外教id',
          teacher_sso_id       string comment '单点登录id',
          teacher_ss_id        string comment '老师私塾id',
          teacher_en_name   string comment '外教英文姓名',
          signed_id bigint comment '签约合同id',
          weekday   bigint comment '星期几,1.0和2.0的算法不一致，考察一下spark的weekday()',
          -- slot_id bigint   comment '车位时间ID',
          start_time string comment '签约时间段开始时间,如09:10',
          end_time string comment '签约时间段结束时间,如09:10',
          start_time_mins bigint comment '签约时间段开始时间',
          end_time_mins   bigint comment '签约时间段结束时间',
          -- is_current bigint comment '是否当前有效签约时间',
          effective_start_date string comment '签约生效开始时间，格式YY-MM-DD',
          effective_end_date  string comment '签约结束时间，格式YY-MM-DD',
          sys       string comment  '1.0,2.0',
          create_time string comment '记录创建时间',
          updated_time string comment '记录更新时间',
          dw_load_time   string comment '加载时间'
     ) comment '外教签约时间段，粒度到每天的时间段';



create table dw.tchops_basic__teacher_cur
(
     teacher_id           bigint comment '主键',
     teacher_sso_id       string comment '单点登录id',
     teacher_ss_id        string comment '老师私塾id',
     teacher_en_name      string  comment '英文名字',
     teacher_cn_name      string comment '中文名字',
     phone                string comment '手机号',
     email                string comment '邮箱,登陆用户名',
     sys                  string comment  '1.0原先的IS_OLD=1, 1.5 原先的IS_OLD=0 ,2.0 2.0外教',
     skype                string comment 'skype 账号',
     bithday              string comment '规范化后的生日如2018-01-01格式',
     country_id           string comment '国家维度表 ',
     is_abroad            bigint comment '居住地国内外区分  1:国内  2:国外',
     time_zone            bigint comment '与UTC时间的时差：值为0-23 弃用',
     sex                  string comment '1 转为M；2 为F',
     introduce            string comment '介绍',
     self_introduce       string comment '教师自我介绍',
     comment              string comment '备注-- 把百分说的拆出来', 
     is_lbest             bigint comment '是否百分说',
     is_disable           bigint comment '是否禁用 1：禁用 对应2.0离职，退休,待录用不取   0：有效。',
     status               bigint comment '老师状态：1 试用老师；2 兼职签约老师 ；3 全职签约老师 ',
     has_passport         bigint comment '是否有护照:0,1',
     has_certificate      bigint comment '是否有教学证书:0,1' ,
     chinese_level        bigint comment '中文程度1零基础2初级3中级4高级',
     teaching_level       bigint comment '适合学生水平1零基础2初级3中级4高级',
     assign_staff_id      bigint comment '绑定外教运营id',
     assign_staff_name    string comment '绑定外教运营名',
     is_bind_one          bigint comment '一对一是否可绑  1 是  2 否  0 无',
     teacher_type         bigint comment '老师类型1外教2中教',
     last_login_ip        string COMMENT '最后登陆IP',
     last_login_at        string COMMENT '最后登陆时间',
     create_time          string comment '记录创建时间',
     update_time          string comment '记录修改时间',
     create_by            bigint comment '创建人',  
     dw_load_time         string comment '加载时间'
) comment '外教当前最新信息';




create table dw.tchops_basic__teacher_his
(
     teacher_id           bigint comment '主键',
     teacher_sso_id       string comment '单点登录id',
     teacher_ss_id        string comment '老师私塾id',
     teacher_en_name      string comment '英文名字',
     email                string comment '邮箱,登陆用户名',
     sys                  string comment  '1.0原先的IS_OLD=1, 1.5 原先的IS_OLD=0 ,2.0 2.0外教',
     skype                string comment 'skype 账号',
     country_id           string comment '国家维度表 ',
     time_zone            bigint comment '与UTC时间的时差：值为0-23 弃用',
     sex                  string comment '1 转为M；2 为F',
     comment              string comment '备注-- 把百分说的拆出来', 
     is_lbest             bigint comment '是否百分说',
     is_disable           bigint comment '是否禁用 1：禁用 对应2.0离职，退休,待录用不取   0：有效。',
     status               bigint comment '老师状态：1 试用老师；2 兼职签约老师 ；3 全职签约老师 ',
     has_certificate      bigint comment '是否有教学证书:0,1' ,
     assign_staff_id      bigint comment '绑定外教运营id',
     assign_staff_name    string comment '绑定外教运营名',
     is_bind_one          bigint comment '一对一是否可绑  1 是    2 否',
     update_time          string comment '记录修改时间',
     dw_start_date        string comment '拉链表开始时间，源字段取自date(load_date),当存历史有信息变化后改为当前时间'
     dw_end_date          string comment '拉链表时间标识，进入DW层结束时间，如果最新的值为‘9999-12-31’，而历史数据则会变为当前日期值',
     dw_load_time         string comment '加载时间'
) comment '取type=1为外教的，拉链表设计时，联合主键为：teacher_id,phone,email,skype,disable,assign_staff_id,status';


create table dw.tchops_basic__teacher_bankinfo_his
( 
     teacher_bank_key bigint comment '外教银行信息代理键',
     teacher_id   bigint comment '外教ID，如需取外教银行信息历史信息时，通过teacher_id关联再加上当时的时间限制就能得到当时的银行账户历史信息。如果取最新的则取dw_end_date='9999-12-31' 。',
     teacher_sso_id   bigint comment '外教单点登录id',
     teacher_ss_id        string comment '老师私塾id',
     has_bank_account   bigint comment '区分是否有paypal或国内银行账户，俩个都有则抛出',
     bank_account_name string comment '银行账户开户名',
     bank_account  string comment '银行账户',
     bank_name string comment '银行名称',
     bank_branch string comment '开户支行',
     has_paypal   bigint comment '区分是否有paypal或国内银行账户，俩个都有则抛出'，
     paypal_account_name string comment 'paypal账户开户名',
     paypal_account string comment 'paypal账户',
     sys       string comment  '1.0,2.0',
     create_time     string  comment '记录创建时间',
     update_time     string  comment '记录修改时间',
     dw_start_date      string comment '拉链表开始时间，源字段取自date(load_date),当存历史有信息变化后改为当前时间',
     dw_end_date        string comment '拉链表时间标识，进入DW层结束时间，如果最新的值为‘9999-12-31’，而历史数据则会变为当前日期值',
     dw_load_time       string comment '加载时间',
) comment '外教的银行账户信息维度表';




