
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator

from settings import default_args


# - 学生账号
# - 基本信息
# - 设备/环境 (测网)
# - 抽取表(学生表)

dag = DAG('restore_test', default_args=default_args,
          schedule_interval='0 1 * * *')

start = SqlSensor(
    task_id='start',
    conn_id='src_main_db',
    sql="SELECT * FROM restore_tracking.restore_log where date(restored_time) = current_date;",
    dag=dag
)



end  = BashOperator(
    task_id='end',
    bash_command='echo date',
    dag=dag)




start >> end
