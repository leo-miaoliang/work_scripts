drop table if exists `tmp`.`tmp_column_his` ;

CREATE TABLE `tmp`.`tmp_column_his` (`TABLE_CATALOG` STRING COMMENT '', `TABLE_SCHEMA` STRING COMMENT '', `TABLE_NAME` STRING COMMENT '', `COLUMN_NAME` STRING COMMENT '', `ORDINAL_POSITION` BIGINT COMMENT '', `COLUMN_DEFAULT` STRING COMMENT '', `IS_NULLABLE` STRING COMMENT '', `DATA_TYPE` STRING COMMENT '', `CHARACTER_MAXIMUM_LENGTH` BIGINT COMMENT '', `CHARACTER_OCTET_LENGTH` BIGINT COMMENT '', `NUMERIC_PRECISION` BIGINT COMMENT '', `NUMERIC_SCALE` BIGINT COMMENT '', `DATETIME_PRECISION` BIGINT COMMENT '', `CHARACTER_SET_NAME` STRING COMMENT '', `COLLATION_NAME` STRING COMMENT '', `COLUMN_TYPE` STRING COMMENT '', `COLUMN_KEY` STRING COMMENT '', `EXTRA` STRING COMMENT '', `PRIVILEGES` STRING COMMENT '', `COLUMN_COMMENT` STRING COMMENT '', `GENERATION_EXPRESSION` STRING COMMENT '', `start_date` STRING, `end_date` STRING)
USING PARQUET;

-- insert change column

insert into tmp.tmp_column_his
select t1.`TABLE_CATALOG`
    ,t1.`TABLE_SCHEMA`
    ,t1.`TABLE_NAME`
    ,t1.`COLUMN_NAME`
    ,t1.`ORDINAL_POSITION`
    ,t1.`COLUMN_DEFAULT`
    ,t1.`IS_NULLABLE`
    ,t1.`DATA_TYPE`
    ,t1.`CHARACTER_MAXIMUM_LENGTH`
    ,t1.`CHARACTER_OCTET_LENGTH`
    ,t1.`NUMERIC_PRECISION`
    ,t1.`NUMERIC_SCALE`
    ,t1.`DATETIME_PRECISION`
    ,t1.`CHARACTER_SET_NAME`
    ,t1.`COLLATION_NAME`
    ,t1.`COLUMN_TYPE`
    ,t1.`COLUMN_KEY`
    ,t1.`EXTRA`
    ,t1.`PRIVILEGES`
    ,t1.`COLUMN_COMMENT`
    ,t1.`GENERATION_EXPRESSION`
    ,t1.`start_date`
    ,'{{ macros.tomorrow_ds(ti) }}' as `end_date`
from dw.information_schema_columns_his t1
left join
    stg.information_schema_columns t2
on  t1.table_schema = t2.table_schema
and t1.table_name = t2.table_name
and t1.column_name = t2.column_name
and t1.data_type = t2.data_type
and t1.column_comment = t2.column_comment
and t2.etl_date='{{ macros.ds(ti) }}'
where t1.end_date = '2999-12-31'
and t2.table_schema is null

;

-- insert nochange column
insert into tmp.tmp_column_his
select t1.`TABLE_CATALOG`
    ,t1.`TABLE_SCHEMA`
    ,t1.`TABLE_NAME`
    ,t1.`COLUMN_NAME`
    ,t1.`ORDINAL_POSITION`
    ,t1.`COLUMN_DEFAULT`
    ,t1.`IS_NULLABLE`
    ,t1.`DATA_TYPE`
    ,t1.`CHARACTER_MAXIMUM_LENGTH`
    ,t1.`CHARACTER_OCTET_LENGTH`
    ,t1.`NUMERIC_PRECISION`
    ,t1.`NUMERIC_SCALE`
    ,t1.`DATETIME_PRECISION`
    ,t1.`CHARACTER_SET_NAME`
    ,t1.`COLLATION_NAME`
    ,t1.`COLUMN_TYPE`
    ,t1.`COLUMN_KEY`
    ,t1.`EXTRA`
    ,t1.`PRIVILEGES`
    ,t1.`COLUMN_COMMENT`
    ,t1.`GENERATION_EXPRESSION`
    ,t1.`start_date`
    ,t1.`end_date`
from dw.information_schema_columns_his t1
left join
    stg.information_schema_columns t2
on  t1.table_schema = t2.table_schema
and t1.table_name = t2.table_name
and t1.column_name = t2.column_name
and t1.data_type = t2.data_type
and t1.column_comment = t2.column_comment
and t2.etl_date='{{ macros.ds(ti) }}'
where t1.end_date = '2999-12-31'
and t2.table_schema is not null

;

-- insert new column
insert into tmp.tmp_column_his
select t1.`TABLE_CATALOG`
    ,t1.`TABLE_SCHEMA`
    ,t1.`TABLE_NAME`
    ,t1.`COLUMN_NAME`
    ,t1.`ORDINAL_POSITION`
    ,t1.`COLUMN_DEFAULT`
    ,t1.`IS_NULLABLE`
    ,t1.`DATA_TYPE`
    ,t1.`CHARACTER_MAXIMUM_LENGTH`
    ,t1.`CHARACTER_OCTET_LENGTH`
    ,t1.`NUMERIC_PRECISION`
    ,t1.`NUMERIC_SCALE`
    ,t1.`DATETIME_PRECISION`
    ,t1.`CHARACTER_SET_NAME`
    ,t1.`COLLATION_NAME`
    ,t1.`COLUMN_TYPE`
    ,t1.`COLUMN_KEY`
    ,t1.`EXTRA`
    ,t1.`PRIVILEGES`
    ,t1.`COLUMN_COMMENT`
    ,t1.`GENERATION_EXPRESSION`
    ,'{{ macros.tomorrow_ds(ti) }}' as start_date
    ,'2999-12-31' as end_date
from stg.information_schema_columns t1
left join
    dw.information_schema_columns_his t2
on  t1.table_schema = t2.table_schema
and t1.table_name = t2.table_name
and t1.column_name = t2.column_name
and t1.data_type = t2.data_type
and t1.column_comment = t2.column_comment
and t2.end_date = '2999-12-31'
where t2.table_schema is null
and t1.etl_date='{{ macros.ds(ti) }}'
;

insert overwrite table dw.information_schema_columns_his
select `TABLE_CATALOG`
    ,`TABLE_SCHEMA`
    ,`TABLE_NAME`
    ,`COLUMN_NAME`
    ,`ORDINAL_POSITION`
    ,`COLUMN_DEFAULT`
    ,`IS_NULLABLE`
    ,`DATA_TYPE`
    ,`CHARACTER_MAXIMUM_LENGTH`
    ,`CHARACTER_OCTET_LENGTH`
    ,`NUMERIC_PRECISION`
    ,`NUMERIC_SCALE`
    ,`DATETIME_PRECISION`
    ,`CHARACTER_SET_NAME`
    ,`COLLATION_NAME`
    ,`COLUMN_TYPE`
    ,`COLUMN_KEY`
    ,`EXTRA`
    ,`PRIVILEGES`
    ,`COLUMN_COMMENT`
    ,`GENERATION_EXPRESSION`
    ,`start_date`
    ,`end_date`
from tmp.tmp_column_his 
; 

