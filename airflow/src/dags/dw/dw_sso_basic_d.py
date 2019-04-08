from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator

from settings import default_args


dag = DAG('dw_sso_basic_d', default_args=default_args,
          schedule_interval='1 1 * * *')

start = SqlSensor(
    task_id='start',
    conn_id='src_main_db',
    sql="SELECT * FROM restore_tracking.restore_log WHERE date(restored_time) = current_date;",
    dag=dag)

# bk_user_info
delpart_stg_bk_user_info = HiveOperator(
    task_id='delpart_stg_bk_user_info',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_user_info drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_bk_user_info = BashOperator(
    task_id='stg_bk_user_info',
    bash_command='dataship extract uuold_sishu.bk_user_info {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_bk_user_info = HiveOperator(
    task_id='addpart_stg_bk_user_info',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_user_info add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_bk_user_info = HiveOperator(
    task_id='stg_ods_bk_user_info',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.sishu_bk_user_info_insert.sql',
    dag=dag)

# bk_user
delpart_stg_bk_user = HiveOperator(
    task_id='delpart_stg_bk_user',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_user drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_bk_user = BashOperator(
    task_id='stg_bk_user',
    bash_command='dataship extract uuold_sishu.bk_user {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_bk_user = HiveOperator(
    task_id='addpart_stg_bk_user',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_user add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_bk_user = HiveOperator(
    task_id='stg_ods_bk_user',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.sishu_bk_user_insert.sql',
    dag=dag)

# bk_user_student
delpart_stg_bk_user_student = HiveOperator(
    task_id='delpart_stg_bk_user_student',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_user_student drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_bk_user_student = BashOperator(
    task_id='stg_bk_user_student',
    bash_command='dataship extract uuold_sishu.bk_user_student {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_bk_user_student = HiveOperator(
    task_id='addpart_stg_bk_user_student',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_user_student add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_bk_user_student = HiveOperator(
    task_id='stg_ods_bk_user_student',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.sishu_bk_user_student_insert.sql',
    dag=dag)


#student_basic
del_partiton_stg_student_user = HiveOperator(
    task_id='del_partiton_stg_student_user',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_student_user drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

src_stg_student_user = BashOperator(
    task_id='src_stg_student_user',
    bash_command='dataship extract uuold_newuuabc.student_user {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

add_partiton_stg_student_user = HiveOperator(
    task_id='add_partiton_stg_student_user',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_student_user add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_student_user = HiveOperator(
    task_id='stg_ods_student_user',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_student_user_insert.sql',
    dag=dag
)


# teacher_user_new

del_partiton_stg_teacher_user_new = HiveOperator(
    task_id='del_partiton_stg_teacher_user_new',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_user_new drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


src_stg_teacher_user_new = BashOperator(
    task_id='src_stg_teacher_user_new',
    bash_command='dataship extract uuold_newuuabc.teacher_user_new {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

add_partiton_stg_teacher_user_new = HiveOperator(
    task_id='add_partiton_stg_teacher_user_new',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_user_new add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_teacher_user_new = HiveOperator(
    task_id='stg_ods_teacher_user_new',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_teacher_user_new_insert.sql',
    dag=dag
)



end = MySqlOperator(
    task_id='end',
    mysql_conn_id='etl_db',
    sql="INSERT INTO `signal` VALUES('{1}', '{0}') ON DUPLICATE KEY UPDATE `value`='{0}'; ".format("{{ macros.ds(ti) }}", "{{ dag.dag_id }}"),
    database='etl',
    dag=dag
)

start >> delpart_stg_bk_user_info >> stg_bk_user_info >> addpart_stg_bk_user_info >> stg_ods_bk_user_info >> end

start >> delpart_stg_bk_user >> stg_bk_user >> addpart_stg_bk_user >> stg_ods_bk_user >> end

start >> delpart_stg_bk_user_student >> stg_bk_user_student >> addpart_stg_bk_user_student >> stg_ods_bk_user_student >> end

start >> del_partiton_stg_student_user >> src_stg_student_user >> add_partiton_stg_student_user >> stg_ods_student_user >> end

start >> del_partiton_stg_teacher_user_new >> src_stg_teacher_user_new >> add_partiton_stg_teacher_user_new >> stg_ods_teacher_user_new >> end