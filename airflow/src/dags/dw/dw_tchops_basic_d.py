from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator

from settings import default_args

# - 合同相关

dag = DAG('dw_tchops_basic_d', default_args=default_args,
          schedule_interval='7 1 * * *')

start = SqlSensor(
    task_id='start',
    conn_id='src_main_db',
    sql="SELECT * FROM restore_tracking.restore_log WHERE date(restored_time) = current_date;",
    dag=dag)



# teacher_signed_log
delpart_stg_teacher_signed_log = HiveOperator(
    task_id='delpart_stg_teacher_signed_log',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_teacher_signed_log drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_teacher_signed_log = BashOperator(
    task_id='stg_teacher_signed_log',
    bash_command='dataship extract uuold_teacher_contract.teacher_signed_log {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_teacher_signed_log = HiveOperator(
    task_id='addpart_stg_teacher_signed_log',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_teacher_signed_log add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_teacher_signed_log = HiveOperator(
    task_id='stg_ods_teacher_signed_log',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.teacher_contract_teacher_signed_log_insert.sql',
    dag=dag
)




# teacher_signed
delpart_stg_teacher_signed = HiveOperator(
    task_id='delpart_stg_teacher_signed',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_teacher_signed drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_teacher_signed = BashOperator(
    task_id='stg_teacher_signed',
    bash_command='dataship extract uuold_teacher_contract.teacher_signed {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_teacher_signed = HiveOperator(
    task_id='addpart_stg_teacher_signed',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_teacher_signed add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_teacher_signed = HiveOperator(
    task_id='stg_ods_teacher_signed',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.teacher_contract_teacher_signed_insert.sql',
    dag=dag
)

# teacher_contract
delpart_stg_teacher_contract = HiveOperator(
    task_id='delpart_stg_teacher_contract',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_teacher_contract drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_teacher_contract = BashOperator(
    task_id='stg_teacher_contract',
    bash_command='dataship extract uuold_teacher_contract.teacher_contract {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_teacher_contract = HiveOperator(
    task_id='addpart_stg_teacher_contract',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_teacher_contract add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_teacher_contract = HiveOperator(
    task_id='stg_ods_teacher_contract',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.teacher_contract_teacher_contract_insert.sql',
    dag=dag
)

# signed_time
delpart_stg_signed_time = HiveOperator(
    task_id='delpart_stg_signed_time',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_signed_time drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_signed_time = BashOperator(
    task_id='stg_signed_time',
    bash_command='dataship extract uuold_teacher_contract.signed_time {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_signed_time = HiveOperator(
    task_id='addpart_stg_signed_time',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_signed_time add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_signed_time = HiveOperator(
    task_id='stg_ods_signed_time',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.teacher_contract_signed_time_insert.sql',
    dag=dag
)



# 20190314 new add of newuuabc teacher contract signed related
# teacher_signed_sys1
delpart_stg_teacher_signed_sys1 = HiveOperator(
    task_id='delpart_stg_teacher_signed_sys1',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_signed drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_teacher_signed_sys1 = BashOperator(
    task_id='stg_teacher_signed_sys1',
    bash_command='dataship extract uuold_newuuabc.teacher_signed {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_teacher_signed_sys1 = HiveOperator(
    task_id='addpart_stg_teacher_signed_sys1',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_signed add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_teacher_signed_sys1 = HiveOperator(
    task_id='stg_ods_teacher_signed_sys1',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_teacher_signed_insert.sql',
    dag=dag)

# teacher_contract_sys1
delpart_stg_teacher_contract_sys1 = HiveOperator(
    task_id='delpart_stg_teacher_contract_sys1',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_contract drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_teacher_contract_sys1 = BashOperator(
    task_id='stg_teacher_contract_sys1',
    bash_command='dataship extract uuold_newuuabc.teacher_contract {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_teacher_contract_sys1 = HiveOperator(
    task_id='addpart_stg_teacher_contract_sys1',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_contract add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_teacher_contract_sys1 = HiveOperator(
    task_id='stg_ods_teacher_contract_sys1',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_teacher_contract_insert.sql',
    dag=dag)

# signed_time_sys1
delpart_stg_signed_time_sys1 = HiveOperator(
    task_id='delpart_stg_signed_time_sys1',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_signed_time drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_signed_time_sys1 = BashOperator(
    task_id='stg_signed_time_sys1',
    bash_command='dataship extract uuold_newuuabc.signed_time {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_signed_time_sys1 = HiveOperator(
    task_id='addpart_stg_signed_time_sys1',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_signed_time add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_signed_time_sys1 = HiveOperator(
    task_id='stg_ods_signed_time_sys1',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_signed_time_insert.sql',
    dag=dag)

end = MySqlOperator(
    task_id='end',
    mysql_conn_id='etl_db',
    sql="INSERT INTO `signal` VALUES('{1}', '{0}') ON DUPLICATE KEY UPDATE `value`='{0}'; ".format("{{ macros.ds(ti) }}", "{{ dag.dag_id }}"),
    database='etl',
    dag=dag
)



start >> delpart_stg_teacher_signed_log >> stg_teacher_signed_log >> addpart_stg_teacher_signed_log >> stg_ods_teacher_signed_log >> end

start >> delpart_stg_teacher_signed >> stg_teacher_signed >> addpart_stg_teacher_signed >> stg_ods_teacher_signed >> end

start >> delpart_stg_teacher_contract >> stg_teacher_contract >> addpart_stg_teacher_contract >> stg_ods_teacher_contract >> end

start >> delpart_stg_signed_time >> stg_signed_time >> addpart_stg_signed_time >> stg_ods_signed_time >> end

start >> delpart_stg_teacher_signed_sys1 >> stg_teacher_signed_sys1 >> addpart_stg_teacher_signed_sys1 >> stg_ods_teacher_signed_sys1 >> end

start >> delpart_stg_teacher_contract_sys1 >> stg_teacher_contract_sys1 >> addpart_stg_teacher_contract_sys1 >> stg_ods_teacher_contract_sys1 >> end

start >> delpart_stg_signed_time_sys1 >> stg_signed_time_sys1 >> addpart_stg_signed_time_sys1 >> stg_ods_signed_time_sys1 >> end




