from datetime import datetime, timedelta

from airflow.settings import TIMEZONE
from airflow.models import Variable

def days_ago(n, hour=0, minute=0, second=0, microsecond=0):
    today = datetime.now(TIMEZONE).replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond)

    return today - timedelta(days=n)


IS_PRD = Variable.get('ENVIRONMENT') == "PRD"

# email setting
ENABLE_EMAIL = IS_PRD
EMAIL_DEFAULT_CC = "bi@uuabc.com; xunhong.wang@uuabc.com"

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(2),
    'email': 'bi@uuabc.com',
    'email_on_failure': ENABLE_EMAIL,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'pool': 'batch_default',
    'priority_weight': 100,
    'catchup': False,
}
