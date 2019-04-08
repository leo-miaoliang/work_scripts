from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from operators.hive_to_localfile_operator import Hive2LocalFileOperator

from settings import default_args, IS_PRD

# set password-less ssh to 47.99.61.134 on prd

dag = DAG('bi_external_onesmart_d', default_args=default_args,
          schedule_interval='0 8 * * *')

export_to_csv = Hive2LocalFileOperator(
    task_id='export_to_csv',
    hiveserver2_conn_id='spark_thrift',
    hql='scripts/rpt.export_student_to_onesmart.sql',
    dst_path='{{ macros.ws(dag_run) }}',
    dst_filename='{{ macros.tomorrow_ds(ti) }}.csv',
    dag=dag)

if IS_PRD:
    # encrypt: tar -czvf - file | openssl des3 -salt -k password -out /path/to/file.tar.gz
    # decrypt: openssl des3 -d -k password -salt -in /path/to/file.tar.gz | tar xzf -
    upload = BashOperator(
        task_id='upload',
        bash_command='''
            cd {{ macros.ws(dag_run) }};
            tar -czvf - *.csv | openssl des3 -salt -k Aa111122 -out ./{{ macros.tomorrow_ds(ti) }}.tar.gz;
            scp *.gz root@47.99.61.134:/data/bigdata
        ''',
        dag=dag)

    export_to_csv >> upload
