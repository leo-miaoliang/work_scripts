import os
import logging

from datetime import timedelta

from airflow.models import Variable
from airflow.settings import TIMEZONE
from airflow.plugins_manager import AirflowPlugin

from operators.hive_to_localfile_operator import Hive2LocalFileOperator

# macros

def ds(ti):
    return TIMEZONE.convert(ti.execution_date).strftime('%Y-%m-%d')

def tomorrow_ds(ti):
    return TIMEZONE.convert(ti.execution_date + timedelta(1)).strftime('%Y-%m-%d')

def yesterday_ds(ti):
    return TIMEZONE.convert(ti.execution_date - timedelta(1)).strftime('%Y-%m-%d')

def refine(run_id):
    return run_id.replace(":", "_").replace("-", "_").replace(".", "_").replace("+", "_")

# get workspace for a dag_run
def ws(dr):
    wspath = Variable.get('WORKSPACE_PATH')
    dag_id = dr.dag_id
    rid = refine(dr.run_id)
    return os.path.join(wspath, dag_id, rid)

class UUabcMacroPlugin(AirflowPlugin):
    name = 'uuabc_plugin'
    operator = [Hive2LocalFileOperator]
    macros = [ds, tomorrow_ds, yesterday_ds, refine, ws]

