
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator

from settings import default_args


# 外教请假旷课

dag = DAG('dw_tchops_perf_d', default_args=default_args,
          schedule_interval='14 1 * * *')

start = SqlSensor(
    task_id='start',
    conn_id='src_main_db',
    sql="SELECT * FROM restore_tracking.restore_log WHERE date(restored_time) = current_date;",
    dag=dag)

# teacher_leave
delpart_stg_teacher_leave = HiveOperator(
    task_id='delpart_stg_teacher_leave',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_leave drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_teacher_leave = BashOperator(
    task_id='stg_teacher_leave',
    bash_command='dataship extract uuold_newuuabc.teacher_leave {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_teacher_leave = HiveOperator(
    task_id='addpart_stg_teacher_leave',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_leave add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_teacher_leave = HiveOperator(
    task_id='stg_ods_teacher_leave',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_teacher_leave_insert.sql',
    dag=dag)

# teacher_absenteeism
delpart_stg_teacher_absenteeism = HiveOperator(
    task_id='delpart_stg_teacher_absenteeism',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_absenteeism drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_teacher_absenteeism = BashOperator(
    task_id='stg_teacher_absenteeism',
    bash_command='dataship extract uuold_newuuabc.teacher_absenteeism {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_teacher_absenteeism = HiveOperator(
    task_id='addpart_stg_teacher_absenteeism',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_absenteeism add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_teacher_absenteeism = HiveOperator(
    task_id='stg_ods_teacher_absenteeism',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_teacher_absenteeism_insert.sql',
    dag=dag)

# bk_leave
delpart_stg_bk_leave = HiveOperator(
    task_id='delpart_stg_bk_leave',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_leave drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_bk_leave = BashOperator(
    task_id='stg_bk_leave',
    bash_command='dataship extract uuold_sishu.bk_leave {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_bk_leave = HiveOperator(
    task_id='addpart_stg_bk_leave',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_leave add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_bk_leave = HiveOperator(
    task_id='stg_ods_bk_leave',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.sishu_bk_leave_insert.sql',
    dag=dag)

# bk_check
delpart_stg_bk_check = HiveOperator(
    task_id='delpart_stg_bk_check',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_check drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_bk_check = BashOperator(
    task_id='stg_bk_check',
    bash_command='dataship extract uuold_sishu.bk_check {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_bk_check = HiveOperator(
    task_id='addpart_stg_bk_check',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_check add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_bk_check = HiveOperator(
    task_id='stg_ods_bk_check',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.sishu_bk_check_insert.sql',
    dag=dag)


wait = DummyOperator(
    task_id='wait',
    dag=dag)

dw_leave = HiveOperator(
    task_id='dw_leave',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/dw/tchops_pref__leave.sql',
    dag=dag)


dw_absence = HiveOperator(
    task_id='dw_absence',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/dw/tchops_pref__absence.sql',
    dag=dag)


end = MySqlOperator(
    task_id='end',
    mysql_conn_id='etl_db',
    sql="INSERT INTO `signal` VALUES('{1}', '{0}') ON DUPLICATE KEY UPDATE `value`='{0}'; ".format(
        "{{ macros.ds(ti) }}", "{{ dag.dag_id }}"),
    database='etl',
    dag=dag
)

start >> delpart_stg_teacher_leave >> stg_teacher_leave >> addpart_stg_teacher_leave >> stg_ods_teacher_leave >> wait

start >> delpart_stg_teacher_absenteeism >> stg_teacher_absenteeism >> addpart_stg_teacher_absenteeism >> stg_ods_teacher_absenteeism >> wait

start >> delpart_stg_bk_leave >> stg_bk_leave >> addpart_stg_bk_leave >> stg_ods_bk_leave >> wait

start >> delpart_stg_bk_check >> stg_bk_check >> addpart_stg_bk_check >> stg_ods_bk_check >> wait

wait >> [dw_leave, dw_absence] >> end
