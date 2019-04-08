from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator

from settings import default_args


# - 销售合同
# - 退费

dag = DAG('dw_commerce_contract_d', default_args=default_args,
          schedule_interval='4 1 * * *')

start = SqlSensor(
    task_id='start',
    conn_id='etl_db',
    sql="SELECT * FROM etl.signal WHERE name='dw_sso_basic_d' AND value='{{ macros.ds(ti) }}';",
    dag=dag
)

# contract

del_partiton_stg_contract = HiveOperator(
    task_id='del_partiton_stg_contract',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_contract drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


src_stg_contract = BashOperator(
    task_id='src_stg_contract',
    bash_command='dataship extract uuold_newuuabc.contract {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

add_partiton_stg_contract = HiveOperator(
    task_id='add_partiton_stg_contract',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_contract add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_contract = HiveOperator(
    task_id='stg_ods_contract',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_contract_insert.sql',
    dag=dag
)

# contract_template

del_partiton_stg_contract_template = HiveOperator(
    task_id='del_partiton_stg_contract_template',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_contract_template drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

src_stg_contract_template = BashOperator(
    task_id='src_stg_contract_template',
    bash_command='dataship extract uuold_newuuabc.contract_template {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

add_partiton_stg_contract_template = HiveOperator(
    task_id='add_partiton_stg_contract_template',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_contract_template add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_contract_template = HiveOperator(
    task_id='stg_ods_contract_template',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_contract_template_insert.sql',
    dag=dag
)

# contract_refund

delpart_stg_contract_refund = HiveOperator(
    task_id='delpart_stg_contract_refund',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_contract_refund drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_contract_refund = BashOperator(
    task_id='stg_contract_refund',
    bash_command='dataship extract uuold_newuuabc.contract_refund {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_contract_refund = HiveOperator(
    task_id='addpart_stg_contract_refund',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_contract_refund add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_contract_refund = HiveOperator(
    task_id='stg_ods_contract_refund',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_contract_refund_insert.sql',
    dag=dag)

# contract_details

delpart_stg_contract_details = HiveOperator(
    task_id='delpart_stg_contract_details',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_contract_details drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_contract_details = BashOperator(
    task_id='stg_contract_details',
    bash_command='dataship extract uuold_newuuabc.contract_details {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_contract_details = HiveOperator(
    task_id='addpart_stg_contract_details',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_contract_details add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_contract_details = HiveOperator(
    task_id='stg_ods_contract_details',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_contract_details_insert.sql',
    dag=dag)

# contract_payment
delpart_stg_contract_payment = HiveOperator(
    task_id='delpart_stg_contract_payment',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_contract_payment drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_contract_payment = BashOperator(
    task_id='stg_contract_payment',
    bash_command='dataship extract uuold_newuuabc.contract_payment {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_contract_payment = HiveOperator(
    task_id='addpart_stg_contract_payment',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_contract_payment add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_contract_payment = HiveOperator(
    task_id='stg_ods_contract_payment',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/ods/stg2ods.newuuabc_contract_payment_insert.sql',
    dag=dag)



wait = DummyOperator(
    task_id='wait',
    dag=dag)


dw_contract = HiveOperator(
    task_id='dw_contract',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/dw/commerce_contract__contract.sql',
    dag=dag)

dw_refund = HiveOperator(
    task_id='dw_refund',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/dw/commerce_contract__refund.sql',
    dag=dag)


end = MySqlOperator(
    task_id='end',
    mysql_conn_id='etl_db',
    sql="INSERT INTO etl.signal VALUES('{1}', '{0}') ON DUPLICATE KEY UPDATE value='{0}'; ".format(
        "{{ macros.ds(ti) }}", "{{ dag.dag_id }}"),
    database='etl',
    dag=dag
)

start >> del_partiton_stg_contract >> src_stg_contract >> add_partiton_stg_contract >> stg_ods_contract >> wait

start >> del_partiton_stg_contract_template >> src_stg_contract_template >> add_partiton_stg_contract_template >> stg_ods_contract_template >> wait

start >> delpart_stg_contract_refund >> stg_contract_refund >> addpart_stg_contract_refund >> stg_ods_contract_refund >> wait

start >> delpart_stg_contract_details >> stg_contract_details >> addpart_stg_contract_details >> stg_ods_contract_details >> wait

start >> delpart_stg_contract_payment >> stg_contract_payment >> addpart_stg_contract_payment >> stg_ods_contract_payment >> wait

wait >> [dw_contract, dw_refund] >> end

