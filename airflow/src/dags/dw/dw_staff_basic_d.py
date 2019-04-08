from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator

from settings import default_args


# - 员工信息
# - admin,部门

dag = DAG('dw_staff_basic_d', default_args=default_args,
          schedule_interval='10 1 * * *')

start = SqlSensor(
    task_id='start',
    conn_id='src_main_db',
    sql="SELECT * FROM restore_tracking.restore_log WHERE date(restored_time) = current_date;",
    dag=dag)

# admin
del_partiton_stg_admin = HiveOperator(
    task_id='del_partiton_stg_admin',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_admin drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


src_stg_admin = BashOperator(
    task_id='src_stg_admin',
    bash_command='dataship extract uuold_newuuabc.admin {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

add_partiton_stg_admin = HiveOperator(
    task_id='add_partiton_stg_admin',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_admin add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_admin = HiveOperator(
    task_id='stg_ods_admin',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_admin_insert.sql',
    dag=dag
)

# department
del_partiton_stg_department = HiveOperator(
    task_id='del_partiton_stg_department',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_department drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


src_stg_department = BashOperator(
    task_id='src_stg_department',
    bash_command='dataship extract uuold_newuuabc.department {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

add_partiton_stg_department = HiveOperator(
    task_id='add_partiton_stg_department',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_department add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_department = HiveOperator(
    task_id='stg_ods_department',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_department_insert.sql',
    dag=dag
)

# bk_department 2.0 sishu's department
delpart_stg_bk_department = HiveOperator(
    task_id='delpart_stg_bk_department',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_department drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_bk_department = BashOperator(
    task_id='stg_bk_department',
    bash_command='dataship extract uuold_sishu.bk_department {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_bk_department = HiveOperator(
    task_id='addpart_stg_bk_department',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_department add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_bk_department = HiveOperator(
    task_id='stg_ods_bk_department',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.sishu_bk_department_insert.sql',
    dag=dag)




end = MySqlOperator(
    task_id='end',
    mysql_conn_id='etl_db',
    sql="INSERT INTO `signal` VALUES('{1}', '{0}') ON DUPLICATE KEY UPDATE `value`='{0}'; ".format("{{ macros.ds(ti) }}", "{{ dag.dag_id }}"),
    database='etl',
    dag=dag
)


start >> del_partiton_stg_admin >> src_stg_admin >> add_partiton_stg_admin >> stg_ods_admin >> end

start >> del_partiton_stg_department >> src_stg_department >> add_partiton_stg_department >> stg_ods_department >> end

start >> delpart_stg_bk_department >> stg_bk_department >> addpart_stg_bk_department >> stg_ods_bk_department >> end
