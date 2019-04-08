# -*- coding: utf-8 -*-
import subprocess



joblist=[
'uuold_newuuabc.department',
'uuold_newuuabc.live_course'
]




# for job in joblist:
#     subprocess.call('dataship autocfg  %s' % job, shell=True)

# for job in joblist:
#
#     subprocess.call('dataship createddl %s' %job,shell=True)
#
# for job in joblist:
#
#     subprocess.call("dataship createddl %s  --style='stg2ods'" %job,shell=True)






sqltext=r'''
# 111
delpart_stg_111 = HiveOperator(
    task_id='delpart_stg_111',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.000_111 drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_111 = BashOperator(
    task_id='stg_111',
    bash_command='dataship extract uuold_000.111 {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_111 = HiveOperator(
    task_id='addpart_stg_111',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.000_111 add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_111 = HiveOperator(
    task_id='stg_ods_111',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.000_111_insert.sql',
    dag=dag)
    
'''

sqltext2='start >> delpart_stg_111 >> stg_111 >> addpart_stg_111 >> stg_ods_111 >> end \n\n'


#


tablelist=[
'sishu.bk_department',
'newuuabc.teacher_signed',
'newuuabc.teacher_contract',
'newuuabc.signed_time'
]


tablelist=['newuuabc.contract_payment']


sqlfinal=''
flow=''
for i in tablelist:
    db,table=i.split('.')
    sqlafter = sqltext.replace('111', table).replace('000', db)
    sqlflow = sqltext2.replace('111', table)
    sqlfinal=sqlfinal+sqlafter
    flow = flow + sqlflow

print(sqlfinal+flow)

