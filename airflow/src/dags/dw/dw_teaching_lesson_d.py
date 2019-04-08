from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator

from settings import default_args

# - 上课记录相关表

dag = DAG('dw_teaching_lesson_d', default_args=default_args,
          schedule_interval='10 1 * * *')


start = SqlSensor(
    task_id='start',
    conn_id='etl_db',
    sql="SELECT * FROM etl.signal WHERE `name`='dw_sso_basic_d' AND `value`='{{ macros.ds(ti) }}';",
    dag=dag
)


# bk_class_times
delpart_stg_bk_class_times = HiveOperator(
    task_id='delpart_stg_bk_class_times',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_class_times drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_bk_class_times = BashOperator(
    task_id='stg_bk_class_times',
    bash_command='dataship extract uuold_sishu.bk_class_times {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_bk_class_times = HiveOperator(
    task_id='addpart_stg_bk_class_times',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_class_times add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_bk_class_times = HiveOperator(
    task_id='stg_ods_bk_class_times',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.sishu_bk_class_times_insert.sql',
    dag=dag)

# bk_class
delpart_stg_bk_class = HiveOperator(
    task_id='delpart_stg_bk_class',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_class drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_bk_class = BashOperator(
    task_id='stg_bk_class',
    bash_command='dataship extract uuold_sishu.bk_class {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_bk_class = HiveOperator(
    task_id='addpart_stg_bk_class',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_class add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_bk_class = HiveOperator(
    task_id='stg_ods_bk_class',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.sishu_bk_class_insert.sql',
    dag=dag)

# bk_sign_in
delpart_stg_bk_sign_in = HiveOperator(
    task_id='delpart_stg_bk_sign_in',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_sign_in drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_bk_sign_in = BashOperator(
    task_id='stg_bk_sign_in',
    bash_command='dataship extract uuold_sishu.bk_sign_in {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_bk_sign_in = HiveOperator(
    task_id='addpart_stg_bk_sign_in',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_sign_in add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_bk_sign_in = HiveOperator(
    task_id='stg_ods_bk_sign_in',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.sishu_bk_sign_in_insert.sql',
    dag=dag)

# bk_subject
delpart_stg_bk_subject = HiveOperator(
    task_id='delpart_stg_bk_subject',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_subject drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_bk_subject = BashOperator(
    task_id='stg_bk_subject',
    bash_command='dataship extract uuold_sishu.bk_subject {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_bk_subject = HiveOperator(
    task_id='addpart_stg_bk_subject',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_subject add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_bk_subject = HiveOperator(
    task_id='stg_ods_bk_subject',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.sishu_bk_subject_insert.sql',
    dag=dag)

# live_course_details
delpart_stg_live_course_details = HiveOperator(
    task_id='delpart_stg_live_course_details',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_live_course_details drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_live_course_details = BashOperator(
    task_id='stg_live_course_details',
    bash_command='dataship extract uuold_newuuabc.live_course_details {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_live_course_details = HiveOperator(
    task_id='addpart_stg_live_course_details',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_live_course_details add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_live_course_details = HiveOperator(
    task_id='stg_ods_live_course_details',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_live_course_details_insert.sql',
    dag=dag)

# bk_subjectdet
delpart_stg_bk_subjectdet = HiveOperator(
    task_id='delpart_stg_bk_subjectdet',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_subjectdet drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_bk_subjectdet = BashOperator(
    task_id='stg_bk_subjectdet',
    bash_command='dataship extract uuold_sishu.bk_subjectdet {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_bk_subjectdet = HiveOperator(
    task_id='addpart_stg_bk_subjectdet',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.sishu_bk_subjectdet add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_bk_subjectdet = HiveOperator(
    task_id='stg_ods_bk_subjectdet',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.sishu_bk_subjectdet_insert.sql',
    dag=dag)

# student_evaluate
delpart_stg_student_evaluate = HiveOperator(
    task_id='delpart_stg_student_evaluate',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_student_evaluate drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_student_evaluate = BashOperator(
    task_id='stg_student_evaluate',
    bash_command='dataship extract uuold_newuuabc.student_evaluate {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_student_evaluate = HiveOperator(
    task_id='addpart_stg_student_evaluate',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_student_evaluate add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_student_evaluate = HiveOperator(
    task_id='stg_ods_student_evaluate',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_student_evaluate_insert.sql',
    dag=dag)

# application_log
delpart_stg_application_log = HiveOperator(
    task_id='delpart_stg_application_log',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_application_log drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_application_log = BashOperator(
    task_id='stg_application_log',
    bash_command='dataship extract uuold_newuuabc.application_log {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_application_log = HiveOperator(
    task_id='addpart_stg_application_log',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_application_log add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_application_log = HiveOperator(
    task_id='stg_ods_application_log',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_application_log_insert.sql',
    dag=dag)

# appoint_course
del_partiton_stg_appoint_course = HiveOperator(
    task_id='del_partiton_stg_appoint_course',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_appoint_course drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


src_stg_appoint_course = BashOperator(
    task_id='src_stg_appoint_course',
    bash_command='dataship extract uuold_newuuabc.appoint_course {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)
add_partiton_stg_appoint_course = HiveOperator(
    task_id='add_partiton_stg_appoint_course',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_appoint_course add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_appoint_course = HiveOperator(
    task_id='stg_ods_appoint_course',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_appoint_course_insert.sql',
    dag=dag
)

# course_details
del_partiton_stg_course_details = HiveOperator(
    task_id='del_partiton_stg_course_details',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_course_details drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


src_stg_course_details = BashOperator(
    task_id='src_stg_course_details',
    bash_command='dataship extract uuold_newuuabc.course_details {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

add_partiton_stg_course_details = HiveOperator(
    task_id='add_partiton_stg_course_details',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_course_details add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_course_details = HiveOperator(
    task_id='stg_ods_course_details',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_course_details_insert.sql',
    dag=dag
)

# student_class
delpart_stg_student_class = HiveOperator(
    task_id='delpart_stg_student_class',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.classbooking_student_class drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_student_class = BashOperator(
    task_id='stg_student_class',
    bash_command='dataship extract uuold_classbooking.student_class {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_student_class = HiveOperator(
    task_id='addpart_stg_student_class',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.classbooking_student_class add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_student_class = HiveOperator(
    task_id='stg_ods_student_class',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.classbooking_student_class_insert.sql',
    dag=dag)

# classroom
delpart_stg_classroom = HiveOperator(
    task_id='delpart_stg_classroom',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.classbooking_classroom drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_classroom = BashOperator(
    task_id='stg_classroom',
    bash_command='dataship extract uuold_classbooking.classroom {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_classroom = HiveOperator(
    task_id='addpart_stg_classroom',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.classbooking_classroom add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_classroom = HiveOperator(
    task_id='stg_ods_classroom',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.classbooking_classroom_insert.sql',
    dag=dag)

# teacher_evaluate
delpart_stg_teacher_evaluate = HiveOperator(
    task_id='delpart_stg_teacher_evaluate',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_evaluate drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_teacher_evaluate = BashOperator(
    task_id='stg_teacher_evaluate',
    bash_command='dataship extract uuold_newuuabc.teacher_evaluate {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_teacher_evaluate = HiveOperator(
    task_id='addpart_stg_teacher_evaluate',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_teacher_evaluate add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_teacher_evaluate = HiveOperator(
    task_id='stg_ods_teacher_evaluate',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_teacher_evaluate_insert.sql',
    dag=dag)

# class_appoint_course
delpart_stg_class_appoint_course = HiveOperator(
    task_id='delpart_stg_class_appoint_course',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_class_appoint_course drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_class_appoint_course = BashOperator(
    task_id='stg_class_appoint_course',
    bash_command='dataship extract uuold_newuuabc.class_appoint_course {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_class_appoint_course = HiveOperator(
    task_id='addpart_stg_class_appoint_course',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_class_appoint_course add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_class_appoint_course = HiveOperator(
    task_id='stg_ods_class_appoint_course',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_class_appoint_course_insert.sql',
    dag=dag)

# live_course
delpart_stg_live_course = HiveOperator(
    task_id='delpart_stg_live_course',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_live_course drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_live_course = BashOperator(
    task_id='stg_live_course',
    bash_command='dataship extract uuold_newuuabc.live_course {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_live_course = HiveOperator(
    task_id='addpart_stg_live_course',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.newuuabc_live_course add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_live_course = HiveOperator(
    task_id='stg_ods_live_course',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.newuuabc_live_course_insert.sql',
    dag=dag)


# carport_time

delpart_stg_carport_time = HiveOperator(
    task_id='delpart_stg_carport_time',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_carport_time drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_carport_time = BashOperator(
    task_id='stg_carport_time',
    bash_command='dataship extract uuold_teacher_contract.carport_time {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_carport_time = HiveOperator(
    task_id='addpart_stg_carport_time',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_carport_time add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_carport_time = HiveOperator(
    task_id='stg_ods_carport_time',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.teacher_contract_carport_time_insert.sql',
    dag=dag
)



wait = DummyOperator(
    task_id='wait',
    dag=dag)

teaching_lesson_class = HiveOperator(
    task_id='teaching_lesson_class',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/dw/teaching_lesson__class.sql',
    dag=dag)

teaching_lesson_student_class = HiveOperator(
    task_id='teaching_lesson_student_class',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/dw.teaching_lesson__student_class.sql',
    dag=dag)

# 20190314 transfer carport_time
delpart_stg_carport_time = HiveOperator(
    task_id='delpart_stg_carport_time',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_carport_time drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_carport_time = BashOperator(
    task_id='stg_carport_time',
    bash_command='dataship extract uuold_teacher_contract.carport_time {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }}',
    pool="embulk_pool",
    dag=dag)

addpart_stg_carport_time = HiveOperator(
    task_id='addpart_stg_carport_time',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.teacher_contract_carport_time add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_ods_carport_time = HiveOperator(
    task_id='stg_ods_carport_time',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.teacher_contract_carport_time_insert.sql',
    dag=dag
)

end = MySqlOperator(
    task_id='end',
    mysql_conn_id='etl_db',
    sql="INSERT INTO `signal` VALUES('{1}', '{0}') ON DUPLICATE KEY UPDATE `value`='{0}'; ".format("{{ macros.ds(ti) }}", "{{ dag.dag_id }}"),
    database='etl',
    dag=dag
)

start >> delpart_stg_bk_class_times >> stg_bk_class_times >> addpart_stg_bk_class_times >> stg_ods_bk_class_times >> wait

start >> delpart_stg_bk_class >> stg_bk_class >> addpart_stg_bk_class >> stg_ods_bk_class >> wait

start >> delpart_stg_bk_sign_in >> stg_bk_sign_in >> addpart_stg_bk_sign_in >> stg_ods_bk_sign_in >> wait

start >> delpart_stg_bk_subject >> stg_bk_subject >> addpart_stg_bk_subject >> stg_ods_bk_subject >> wait

start >> delpart_stg_live_course_details >> stg_live_course_details >> addpart_stg_live_course_details >> stg_ods_live_course_details >> wait

start >> delpart_stg_bk_subjectdet >> stg_bk_subjectdet >> addpart_stg_bk_subjectdet >> stg_ods_bk_subjectdet >> wait

start >> delpart_stg_student_evaluate >> stg_student_evaluate >> addpart_stg_student_evaluate >> stg_ods_student_evaluate >> wait

start >> delpart_stg_application_log >> stg_application_log >> addpart_stg_application_log >> stg_ods_application_log >> wait

start >> del_partiton_stg_appoint_course >> src_stg_appoint_course >> add_partiton_stg_appoint_course >> stg_ods_appoint_course >> wait

start >> del_partiton_stg_course_details >> src_stg_course_details >> add_partiton_stg_course_details >> stg_ods_course_details >> wait

start >> delpart_stg_student_class >> stg_student_class >> addpart_stg_student_class >> stg_ods_student_class >> wait

start >> delpart_stg_classroom >> stg_classroom >> addpart_stg_classroom >> stg_ods_classroom >> wait

start >> delpart_stg_teacher_evaluate >> stg_teacher_evaluate >> addpart_stg_teacher_evaluate >> stg_ods_teacher_evaluate >> wait

start >> delpart_stg_class_appoint_course >> stg_class_appoint_course >> addpart_stg_class_appoint_course >> stg_ods_class_appoint_course >> wait

start >> delpart_stg_live_course >> stg_live_course >> addpart_stg_live_course >> stg_ods_live_course >> wait

start >> delpart_stg_carport_time >> stg_carport_time >> addpart_stg_carport_time >> stg_ods_carport_time >> wait

wait >> teaching_lesson_class >> end

wait >> teaching_lesson_student_class >> end
