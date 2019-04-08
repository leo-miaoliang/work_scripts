import os
import json
import logging
from datetime import datetime, timedelta

from airflow.settings import TIMEZONE
from airflow.utils.email import send_email

from settings import IS_PRD, ENABLE_EMAIL, EMAIL_DEFAULT_CC
from register import ds

EMAIL_TMPL = """
    <html>
        <head>
            <title>BI 自动邮件</title>
            <meta charset="utf-8"/>
        </head>
        <body>
            <div>
            Hi all,<br/>
            各位好，<br/><br/>
            </div>
            <div style="padding-left:20px;text-indent:2em">
                <div>
                    <p>{0}</p>
                </div>
                <div>
                    <p>&nbsp;</p>
                    <p>这是一封自动发送的邮件，请不要直接回复本邮件。</p>
                    <p>This email is automatically sent by system, so please don't reply. </p>
                    <p>如果您有数据需求或其他事务需要联系我们，请发邮件到 bi@uuabc.com 或 xunhong.wang@uuabc.com。</p>
                    <p>If you have any data requirements, please don’t hesitate to contact Big Data Team or xunhong.wang@uuabc.com.</p>
                </div>
            </div>
            <div>
                Thanks,<br/>
                Big Data Team
            </div>
        </body>
    </html>
"""

def days_ago(n, hour=0, minute=0, second=0, microsecond=0):
    today = datetime.now(TIMEZONE).replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond)

    return today - timedelta(days=n)


def generate_spark_args(obj):
  if not isinstance(obj, dict):
    raise TypeError("obj should be a dict")

  args = []
  for k,v in obj.items():
    args.append('--' + k)
    args.append(v)

  return args


def read_script(path, filename):
    with open(os.path.join(path, filename) \
        , encoding='utf-8') as f:
        return f.read()


def mail_files(to, subject, debug_to=None, content='', cc=None, file_task='gen_excel', **kwargs):
    if not ENABLE_EMAIL and not debug_to:
        return

    result_txt = kwargs['ti'].xcom_pull(task_ids=file_task)
    logging.info(result_txt)
    result = json.loads(result_txt)

    if result['status'] != 'success' or \
        len(result['data']['files']) == 0:
        return

    files = result['data']['files']
    subject = subject.format(ds=ds(kwargs['ti']))
    cc = cc + EMAIL_DEFAULT_CC if cc else EMAIL_DEFAULT_CC
    html_content = EMAIL_TMPL.format(content)

    if debug_to and not IS_PRD:
        to = debug_to
        cc = None

    send_email(to=to
        , subject=subject
        , html_content=html_content
        , files=files
        , cc=cc
        , mime_charset='utf-8'
    )
