from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from common.utils import generate_spark_args
from settings import default_args


dag = DAG('bi_sales_tmk_d', default_args=default_args,
          schedule_interval='0 4 * * *')

wait_dw_commerce_contract = SqlSensor(
    task_id='wait_dw_commerce_contract',
    conn_id='etl_db',
    sql="SELECT * FROM etl.signal WHERE `name`='dw_commerce_contract_d' AND `value`='{{ macros.ds(ti) }}';",
    dag=dag
)

wait_dw_student_basic = SqlSensor(
    task_id='wait_dw_student_basic',
    conn_id='etl_db',
    sql="SELECT * FROM etl.signal WHERE `name`='dw_student_basic_d' AND `value`='{{ macros.ds(ti) }}';",
    dag=dag
)

wait_dw_teaching_lesson = SqlSensor(
    task_id='wait_dw_teaching_lesson',
    conn_id='etl_db',
    sql="SELECT * FROM etl.signal WHERE `name`='dw_teaching_lesson_d' AND `value`='{{ macros.ds(ti) }}';",
    dag=dag
)

wait_dw_staff_basic = SqlSensor(
    task_id='wait_dw_staff_basic',
    conn_id='etl_db',
    sql="SELECT * FROM etl.signal WHERE `name`='dw_staff_basic_d' AND `value`='{{ macros.ds(ti) }}';",
    dag=dag
)

start = DummyOperator(task_id='start', dag=dag)

dm_sales_trail_course_by_cc_stat = HiveOperator(
    task_id='dm_sales_trail_course_by_cc_stat',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/dm.sales_trail_course_by_cc_stat.sql',
    dag=dag
)

sync_to_pg = SparkSubmitOperator(
    task_id='sync_to_pg',
    application="hdfs://hdfscluster/apps/spark/apps/spark-etl-assembly-1.6.1.jar",
    java_class="com.uuabc.etl.apps.PGSalesTrailCourseByCCStat",
    name="pg_sales_trail_course_by_cc_stat",
    total_executor_cores=3,
    executor_cores=1,
    executor_memory="2G",
    driver_memory="1G",
    verbose=True,
    pool="spark_submit_pool",
    application_args=generate_spark_args({
        "workflow_id": "{{ macros.refine(run_id) }}",
        "pg_conn_url": "{{ var.value.PG_CONN_URL }}"
    }),
    dag=dag)

end = DummyOperator(task_id='end', dag=dag)

[wait_dw_commerce_contract, wait_dw_student_basic, wait_dw_teaching_lesson, wait_dw_staff_basic] >> start >> dm_sales_trail_course_by_cc_stat >> sync_to_pg >> end
