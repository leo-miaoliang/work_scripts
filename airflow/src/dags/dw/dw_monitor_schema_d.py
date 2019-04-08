
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator

from settings import default_args


# - 每日抽取schema数据,并监控其schema变化

dag = DAG('dw_monitor_schema_d',
          default_args=default_args,
          schedule_interval='49 0 * * *')

start = SqlSensor(
    task_id='start',
    conn_id='src_main_db',
    sql="SELECT * FROM restore_tracking.restore_log WHERE date(restored_time) = current_date;",
    dag=dag)

#columns
del_partiton_stg_schema_columns = HiveOperator(
    task_id='del_partiton_stg_schema_columns',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.information_schema_columns drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    hiveconf_jinja_translate=True,
    dag=dag)

src_stg_schema_columns = BashOperator(
    task_id='src_stg_schema_columns',
    bash_command='dataship extract uuold_information_schema.columns {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    dag=dag)

add_partiton_stg_schema_columns = HiveOperator(
    task_id='add_partiton_stg_schema_columns',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.information_schema_columns add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    hiveconf_jinja_translate=True,
    dag=dag)

# tables
del_partiton_stg_schema_tables = HiveOperator(
    task_id='del_partiton_stg_schema_tables',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.information_schema_tables drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    hiveconf_jinja_translate=True,
    dag=dag)

src_stg_schema_tables = BashOperator(
    task_id='src_stg_schema_tables',
    bash_command='dataship extract uuold_information_schema.tables {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    dag=dag)

add_partiton_stg_schema_tables = HiveOperator(
    task_id='add_partiton_stg_schema_tables',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.information_schema_tables add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    hiveconf_jinja_translate=True,
    dag=dag)

#读取到dw层做拉链表
stg_dw_columns_his = HiveOperator(
    task_id='stg_dw_columns_his',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2dw.information_schema_columns_his.sql',
    hiveconf_jinja_translate=True,
    dag=dag
)

end = MySqlOperator(
    task_id='end',
    mysql_conn_id='etl_db',
    sql="INSERT INTO `signal` VALUES('{1}', '{0}') ON DUPLICATE KEY UPDATE `value`='{0}'; ".format("{{ macros.ds(ti) }}", "{{ dag.dag_id }}"),
    database='etl',
    dag=dag
)


start >> del_partiton_stg_schema_columns >> src_stg_schema_columns >> add_partiton_stg_schema_columns >> stg_dw_columns_his >> end

start >> del_partiton_stg_schema_tables >> src_stg_schema_tables >> add_partiton_stg_schema_tables >> stg_dw_columns_his