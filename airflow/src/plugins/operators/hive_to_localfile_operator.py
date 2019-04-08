import os
import tempfile
import shutil

from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class Hive2LocalFileOperator(BaseOperator):
    template_fields = ('hql', 'dst_path', 'dst_filename')
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(
            self, hql,
            dst_path,
            dst_filename=None,
            hiveserver2_conn_id='hiveserver2_default',
            *args, **kwargs):
        super(Hive2LocalFileOperator, self).__init__(*args, **kwargs)

        self.hiveserver2_conn_id = hiveserver2_conn_id
        self.dst_path = dst_path
        self.dst_filename = dst_filename
        self.hql = hql.strip().rstrip(';')

    def execute(self, context):
        hive = HiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id)
        tmpfile = tempfile.NamedTemporaryFile()
        self.log.info("Fetching file from Hive")

        hive.to_csv(hql=self.hql, csv_filepath=tmpfile.name)
        self.log.info("Pushing to localfile")

        if not os.path.exists(self.dst_path):
            os.makedirs(self.dst_path)

        target = os.path.join(self.dst_path,
                              self.dst_filename) if self.dst_filename else self.dst_path

        shutil.copy(tmpfile.name, target)