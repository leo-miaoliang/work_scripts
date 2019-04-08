import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

from settings import default_args
from register import ws


# - mac address

default_args.update({'start_date': timezone.datetime(2018, 12, 21)})
dag = DAG('dw_sales_leads_d',
          default_args=default_args,
          schedule_interval='20 0 * * *')


start = DummyOperator(
    task_id='start',
    dag=dag)

download_csv = BashOperator(
    task_id='download_csv',
    bash_command='''
        url='http://files.51uuabc.com/tmk/mac_addr_data/output/{{ macros.ds(ti) }}/mac.csv'
        status_code=$(curl --write-out %{http_code} --silent --output /dev/null $url)
        if [[ "$status_code" -eq 200 ]] ; then
            mkdir -p {{ macros.ws(dag_run) }}'/'
            wget $url -O {{ macros.ws(dag_run) }}'/macfile.csv'
            exit 0
        fi
    ''',
    dag=dag)

def is_file_exists(**kwargs):
    dag_run = kwargs['dag_run']
    filepath = os.path.join(ws(dag_run), 'macfile.csv')

    return "delpart_stg_mac" if os.path.isfile(filepath) else "go_easy_mac"


file_exist = BranchPythonOperator(
    task_id='file_exist',
    provide_context=True,
    python_callable=is_file_exists,
    dag=dag
)

go_easy_mac = DummyOperator(
    task_id='go_easy_mac',
    dag=dag)

guard_mac = DummyOperator(
    task_id='guard_mac',
    trigger_rule='one_success',
    dag=dag,
)

delpart_stg_mac = HiveOperator(
    task_id='delpart_stg_mac',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.leads_mac drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_mac = BashOperator(
    task_id='stg_mac',
    bash_command='''
        dataship extract csvfile_leads.mac {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }} --filepath={{ macros.ws(dag_run) }}'/macfile'
    ''',
    pool="embulk_pool",
    dag=dag)

addpart_stg_mac = HiveOperator(
    task_id='addpart_stg_mac',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.leads_mac add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

stg_ods_leads_mac = HiveOperator(
    task_id='stg_ods_leads_mac',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.leads_mac.sql',
    dag=dag
)

#  - extract mac json file

download_json = BashOperator(
    task_id='download_json',
    bash_command='''
        ws='{{ macros.ws(dag_run) }}/mac_addr/'
        url='http://files.51uuabc.com/tmk/mac_addr_data/mac_addr/{{ macros.ds(ti) }}/'
        files=`curl $url | grep "tgz" | awk -F '"' '{print $2}' | sed -e 's/[[:space:]]//g'`

        if [  -n "$files" ]; then
            mkdir -p  $ws
            cd $ws

            # download
            for gzfile in ${files}
            do
                curl -O $url$gzfile
                tar -zxvf $gzfile
            done
            # merge
            cd ..
            find . -type f -regex ".*/[0-9]*" -exec 'cat' {} \; > final_json
        fi
    ''',
    dag=dag)

def is_jsonfile_exists(**kwargs):
    dag_run = kwargs['dag_run']
    filepath = os.path.join(ws(dag_run),'final_json')

    return "delpart_stg_mac_addr" if os.path.isfile(filepath) else "go_easy_mac_addr"


jsonfile_exist = BranchPythonOperator(
    task_id='jsonfile_exist',
    provide_context=True,
    python_callable=is_jsonfile_exists,
    dag=dag
)

go_easy_mac_addr = DummyOperator(
    task_id='go_easy_mac_addr',
    dag=dag)

guard_mac_addr = DummyOperator(
    task_id='guard_mac_addr',
    trigger_rule='one_success',
    dag=dag,
)

delpart_stg_mac_addr = HiveOperator(
    task_id='delpart_stg_mac_addr',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.leads_mac_addr drop if exists PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)


stg_mac_addr = BashOperator(
    task_id='stg_mac_addr',
    bash_command='''
        dataship extract jsonfile_leads.mac_addr {{ macros.ds(ti) }} {{ macros.tomorrow_ds(ti) }} --filepath={{ macros.ws(dag_run) }}'/final_json'
    ''',
    pool="embulk_pool",
    dag=dag)

addpart_stg_mac_addr = HiveOperator(
    task_id='addpart_stg_mac_addr',
    hive_cli_conn_id='spark_thrift',
    hql="alter table stg.leads_mac_addr add PARTITION (etl_date='{{ macros.ds(ti) }}');\n ",
    dag=dag)

ods_mac_addr = HiveOperator(
    task_id='ods_mac_addr',
    hive_cli_conn_id='spark_thrift',
    hql='scripts/stg2ods.leads_mac_addr.sql',
    dag=dag
)

end = MySqlOperator(
    task_id='end',
    mysql_conn_id='etl_db',
    sql="INSERT INTO `signal` VALUES('{1}', '{0}') ON DUPLICATE KEY UPDATE `value`='{0}'; ".format(
        "{{ macros.ds(ti) }}", "{{ dag.dag_id }}"),
    database='etl',
    dag=dag
)



start >> download_csv >> file_exist >> delpart_stg_mac >> stg_mac >> addpart_stg_mac >> stg_ods_leads_mac >> guard_mac >> end
file_exist >> go_easy_mac >> guard_mac


start >> download_json >> jsonfile_exist >> delpart_stg_mac_addr >> stg_mac_addr >> addpart_stg_mac_addr >> ods_mac_addr >> guard_mac_addr >> end
jsonfile_exist >> go_easy_mac_addr >> guard_mac_addr

