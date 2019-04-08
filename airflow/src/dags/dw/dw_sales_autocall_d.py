
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator

from settings import default_args



dag = DAG('dw_sales_autocall_d', default_args=default_args,
          schedule_interval='25 1 * * *')

#begin
start = DummyOperator(
    task_id='start',
    dag=dag)

# -basic table
delpart_stg_webhook_data = HiveOperator(
    task_id='delpart_stg_webhook_data',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.mangguo_webhook_data drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_webhook_data = BashOperator(
    task_id='stg_webhook_data',
    bash_command='dataship extract callsale_mangguo_webhook.webhook_data {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    dag=dag)

addpart_stg_webhook_data = HiveOperator(
    task_id='addpart_stg_webhook_data',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.mangguo_webhook_data add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

ods_webhook_data = HiveOperator(
    task_id='ods_webhook_data',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.webhook_data_insert.sql',
    dag=dag
)

# -callitem

delpart_stg_webhook_data_callitem = HiveOperator(
    task_id='delpart_stg_webhook_data_callitem',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.mangguo_webhook_data_callitem drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_webhook_data_callitem = BashOperator(
    task_id='stg_webhook_data_callitem',
    bash_command='dataship extract callsale_mangguo_webhook.webhook_data_callitem {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    dag=dag)

addpart_stg_webhook_data_callitem = HiveOperator(
    task_id='addpart_stg_webhook_data_callitem',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.mangguo_webhook_data_callitem add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

ods_webhook_data_callitem = HiveOperator(
    task_id='ods_webhook_data_callitem',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.webhook_data_callitem_insert.sql',
    dag=dag
)

# -callitem->flowinfo

delpart_stg_webhook_data_callitem_flowinfo = HiveOperator(
    task_id='delpart_stg_webhook_data_callitem_flowinfo',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.mangguo_webhook_data_callitem_flowinfo drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_webhook_data_callitem_flowinfo = BashOperator(
    task_id='stg_webhook_data_callitem_flowinfo',
    bash_command='dataship extract callsale_mangguo_webhook.webhook_data_callitem_flowinfo {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    dag=dag)

addpart_stg_webhook_data_callitem_flowinfo = HiveOperator(
    task_id='addpart_stg_webhook_data_callitem_flowinfo',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.mangguo_webhook_data_callitem_flowinfo add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

ods_webhook_data_callitem_flowinfo = HiveOperator(
    task_id='ods_webhook_data_callitem_flowinfo',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.webhook_data_callitem_flowinfo_insert.sql',
    dag=dag
)

# end
end = MySqlOperator(
    task_id='end',
    mysql_conn_id='etl_db',
    sql="INSERT INTO `signal` VALUES('{1}', '{0}') ON DUPLICATE KEY UPDATE `value`='{0}'; ".format("{{ macros.ds(ti) }}", "{{ dag.dag_id }}"),
    database='etl',
    dag=dag
)


start >> delpart_stg_webhook_data >> stg_webhook_data >> addpart_stg_webhook_data >> ods_webhook_data >> end

start >> delpart_stg_webhook_data_callitem >> stg_webhook_data_callitem >> addpart_stg_webhook_data_callitem >> ods_webhook_data_callitem >> end

start >> delpart_stg_webhook_data_callitem_flowinfo >> stg_webhook_data_callitem_flowinfo >> addpart_stg_webhook_data_callitem_flowinfo >> ods_webhook_data_callitem_flowinfo >> end
