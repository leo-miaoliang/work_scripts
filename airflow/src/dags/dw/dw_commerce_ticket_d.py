from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator

from settings import default_args


# - 学生课时
# - 课时消耗

dag = DAG('dw_commerce_ticket_d', default_args=default_args,
          schedule_interval='10 1 * * *')

start = SqlSensor(
    task_id='start',
    conn_id='src_main_db',
    sql="SELECT * FROM restore_tracking.restore_log WHERE date(restored_time) = current_date;",
    dag=dag)


# product_consume
delpart_stg_product_consume = HiveOperator(
    task_id='delpart_stg_product_consume',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_product_consume drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_product_consume = BashOperator(
    task_id='stg_product_consume',
    bash_command='dataship extract uuold_newuuabc.product_consume {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_product_consume = HiveOperator(
    task_id='addpart_stg_product_consume',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_product_consume add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_product_consume = HiveOperator(
    task_id='stg_ods_product_consume',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_product_consume_insert.sql',
    dag=dag)


# school_hour
del_partiton_stg_school_hour = HiveOperator(
    task_id='del_partiton_stg_school_hour',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_school_hour drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


src_stg_school_hour = BashOperator(
    task_id='src_stg_school_hour',
    bash_command='dataship extract uuold_newuuabc.school_hour {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

add_partiton_stg_school_hour = HiveOperator(
    task_id='add_partiton_stg_school_hour',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_school_hour add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_school_hour = HiveOperator(
    task_id='stg_ods_school_hour',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_school_hour_insert.sql',
    dag=dag
)

end = MySqlOperator(
    task_id='end',
    mysql_conn_id='etl_db',
    sql="INSERT INTO `signal` VALUES('{1}', '{0}') ON DUPLICATE KEY UPDATE `value`='{0}'; ".format("{{ macros.ds(ti) }}", "{{ dag.dag_id }}"),
    database='etl',
    dag=dag
)


start >> delpart_stg_product_consume >> stg_product_consume >> addpart_stg_product_consume >> stg_ods_product_consume >> end

start >> del_partiton_stg_school_hour >> src_stg_school_hour >> add_partiton_stg_school_hour >> stg_ods_school_hour >> end
