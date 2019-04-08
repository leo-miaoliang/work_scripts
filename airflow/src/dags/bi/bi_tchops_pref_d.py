import os
import json

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator

from settings import default_args
from common.utils import on_prd, read_script
from common.mail import mail_files

current_dir = os.path.dirname(os.path.abspath(__file__))


dag = DAG('bi_tchops_pref_d', default_args=default_args,
          schedule_interval=on_prd('5 8 * * *'))

start = SqlSensor(
    task_id='start',
    conn_id='src_main_db',
    sql="SELECT * FROM restore_tracking.restore_log WHERE date(restored_time) >= '{{ macros.ds(ti) }}';",
    dag=dag
)

# gen_excel=SimpleHttpOperator(
#     task_id='gen_excel',
#     http_conn_id='taskhub_host',
#     endpoint='excel',
#     data=json.dumps([
#         {
#             "sql": read_script(current_dir, "scripts/rpt.export_student_to_onesmart.sql"),
#             "filename": "testx01"
#         },
#         {
#             "header": "a,b",
#             "sql": "SELECT id, flag from ods.newuuabc_student_user limit 11",
#             "filename": "testx02"
#         }
#     ]),
#     log_response=True,
#     xcom_push=True,
#     headers={"Content-Type": "application/json"},
#     response_check=lambda response: True if response.status_code == 200 else False,
#     dag=dag
# )


gen_excel = SimpleHttpOperator(
    task_id='gen_excel',
    http_conn_id='taskhub_host',
    endpoint='task',
    data=json.dumps(
        {
            "name": "teacher-pref-d",
            "params": {"MMdd": "{{ macros.ds(ti) }}"}
        }),
    log_response=True,
    xcom_push=True,
    headers={"Content-Type": "application/json"},
    response_check=lambda response: True if response.status_code == 200 else False,
    dag=dag
)

send_excel = PythonOperator(
    task_id='send_excel',
    python_callable=mail_files,
    op_kwargs={
        'to': "yanying.ke@uuabc.com; jingtong.wang@uuabc.com; kan.liu@uuabc.com; willconlydwyer@uuabc.com; rong.liu@uuabc.com; ping.he@uuabc.com",
        'subject': '{ds} - Teacher Attendance Daily Reports 外教出勤日报',
        'debug_to': 'xunhong.wang@uuabc.com',
        'content': """
                <p>请于附件中查收昨日外教出勤日报。</p>
                <p>Please find the teacher attendance reports of the previous day in the attachments.</p>
            """
    },
    provide_context=True,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> gen_excel >> send_excel >> end
