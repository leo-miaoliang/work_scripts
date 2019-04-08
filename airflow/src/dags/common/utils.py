import os
from settings import IS_PRD

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

def on_prd(cron_conf):
    return cron_conf if IS_PRD else None


