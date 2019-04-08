insert overwrite table dm.schema_columns_changed partition (etl_date = '{{ macros.ds(ti) }}',mysql_source= 'uuold')
select 
    a1.table_schema
   ,a1.table_name
   ,a1.column_name
   ,a1.data_type
   ,a1.column_comment
   ,a1.start_date
   ,a1.end_date
   ,case when a2.table_schema is null  and a1.remark='column_newinsert' then 'new_table' else a1.remark end remark
from(
       select 
            table_schema
           ,table_name
           ,column_name
           ,data_type
           ,column_comment
           ,start_date
           ,end_date
           ,case when rn>1 and end_date='2999-12-31' then 'comment_changed'
                 when rn+cnt=2 and end_date='{{ macros.tomorrow_ds(ti) }}' then 'column_deleted'
                 when rn+cnt=2 and end_date='2999-12-31' then 'column_newinsert' end as remark
       from 
       (  select table_schema
             ,table_name
             ,column_name
             ,data_type
             ,column_comment
             ,start_date
             ,end_date
             ,ROW_NUMBER()over(PARTITION by table_schema,table_name,column_name,data_type order by end_date ) as rn
             ,count(1) over(PARTITION by table_schema,table_name,column_name,data_type) as cnt
          from dw.information_schema_columns_his  t1
          where ( start_date='{{ macros.tomorrow_ds(ti) }}' or end_date='{{ macros.tomorrow_ds(ti) }}')
       ) t
       where  rn=cnt
    ) a1
left join 
    (select 
          table_schema
         ,table_name
     from dw.information_schema_columns_his 
     where start_date<'{{ macros.tomorrow_ds(ti) }}'
     group by  table_schema,table_name
    ) a2 
on a1.table_schema=a2.table_schema and a1.table_name=a2.table_name
order by a1.table_schema,a1.table_name
; 
