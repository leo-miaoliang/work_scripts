
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator

from settings import default_args


# - 学生账号
# - 基本信息
# - 设备/环境 (测网)
# - 学生表(2019-03-11移动sso的用户dag下)
# - 家长表

dag = DAG('dw_student_basic_d', default_args=default_args,
          schedule_interval='0 1 * * *')

start = SqlSensor(
    task_id='start',
    conn_id='src_main_db',
    sql="SELECT * FROM restore_tracking.restore_log WHERE date(restored_time) = current_date;",
    dag=dag)



del_partiton_stg_parents = HiveOperator(
    task_id='del_partiton_stg_parents',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_parents drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

src_stg_parents = BashOperator(
    task_id='src_stg_parents',
    bash_command='dataship extract uuold_newuuabc.parents {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

add_partiton_stg_parents = HiveOperator(
    task_id='add_partiton_stg_parents',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_parents add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_parents = HiveOperator(
    task_id='stg_ods_parents',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_parents_insert.sql',
    dag=dag
)

end = MySqlOperator(
    task_id='end',
    mysql_conn_id='etl_db',
    sql="INSERT INTO `signal` VALUES('{1}', '{0}') ON DUPLICATE KEY UPDATE `value`='{0}'; ".format("{{ macros.ds(ti) }}", "{{ dag.dag_id }}"),
    database='etl',
    dag=dag
)


start >> del_partiton_stg_parents >> src_stg_parents >> add_partiton_stg_parents >> stg_ods_parents >> end
