from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from utils import generate_spark_args
from settings import default_args


dag = DAG('bi_monitor_schema_columns_d'
          , default_args=default_args,
          schedule_interval='54 0 * * *')

wait_dw_schema_monitor = SqlSensor(
    task_id='wait_dw_schema_monitor',
    conn_id='etl_db',
    sql="SELECT * FROM etl.signal WHERE `name`='dw_monitor_schema_d' AND `value`='{{ macros.ds(ti) }}';",
    dag=dag
)


dw_dm_columns_changed = HiveOperator(
    task_id='dw_dm_columns_changed',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/dm.monitor_schema_col_changed.sql',
    hiveconf_jinja_translate=True,
    dag=dag
)


#从spark中读取comment变更了的字段，如果有则将alter语句写入到固定目录下；若有除comment以外的字段变化，则邮件抛出


end = DummyOperator(task_id='end', dag=dag)

wait_dw_schema_monitor >>  dw_dm_columns_changed >> end
