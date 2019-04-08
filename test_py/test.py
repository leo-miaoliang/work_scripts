# -*- coding: utf-8 -*-
from datetime import datetime
import sys
import os
#path = os.path.dirname(os.path.realpath(__file__))
path = os.getcwd()
print(path)
a = []
for i in [1, 2, 3]:
    a.append(i)
print(a)
job = 'uuold_newuu_id'
con_type, tableid = job.split("_", 1)
print(con_type, tableid)
tableschema = 'newuuabc'
incre_col = 1
if tableschema == 'newuuabc' and incre_col:
    incre_col = incre_col + 29
    print(incre_col)
else:
    print('fuck')


a = "{} {}".format("hello", "world")
print(a)

a = [45, 2, 3, 4]
for idx, name in enumerate(a):
    print(idx, name+1)


print(os.getcwd())
#
#
# it = iter(list)    # 创建迭代器对象
# for x in it:
#     print (x, end=" ")

a = map(lambda x: x * 2, [1, 2, 3])
print(list(a))

b = [-1, -34, -5, -56]
c = list(map(abs, b))
print(c)


class Test(object):
    def __init__(self, stu_name, age):
        self.name = stu_name
        self.age = age

    def print(self):
        print(self.name, self.age)


cl = Test('leo', 23)
print(cl.name)

c = cl.name if len(c) < 0 else 1
print(c)
# timestamp


test = [['a', 2, 3], ['b', 4, 5], ['a', 6, 7]]

a = [{name: age} for [name, age, _] in test if name == 'a']

dict_a = [{'a': 1}, {'b': 2}]
for i in dict_a:
    for key in i:
        if key == 'b':
            print('dict test result:%s' % i[key])


incre_col = 'parent_test'


col_dict = [{'id': 'int'}, {'uuid': 'bigint'}, {'salt': 'int'}, {'age': 'int'}, {'diamond_total': 'int'}, {'diamond': 'int'}, {'leave': 'int'}, {'level': 'int'}, {'province': 'int'}, {'city': 'int'}, {'balance': 'int'}, {'channel': 'int'}, {'source': 'int'}, {'last_login': 'int'}, {'create_time': 'int'}, {'assign_consultant': 'int'}, {'assign_teacher': 'int'}, {'founder': 'int'}, {'recommended': 'int'}, {'willingness': 'int'}, {'customer_type': 'int'}, {'nettest': 'int'}, {'student_test': 'int'},
            {'parent_test': 'int'}, {'testlisten': 'int'}, {'netlisten': 'int'}, {'artificial_level': 'int'}, {'level_status': 'int'}, {'test_level': 'int'}, {'update_at': 'timestamp'}]
col_type = ''
for i in col_dict:
    for key in i:
        if key == incre_col:
            col_type = i[key]
            break
    if col_type:
        break
print(col_type)
if col_type == '':
    print("Incre Column: %s  ,it's datatype not in ('timestamp','datetime','int','bigint')" % incre_col)
    sys.exit(1)


list_aaa = [['a', '111'], ['aa', 'bb']]
col_dict1 = {col_name: data_type for col_name, data_type in list_aaa}

if col_dict1.get('bb', ''):
    print(col_dict1.get('a'))


print(col_dict1)


a = datetime(2018, 12, 24)
print(a.weekday())

weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday',
            'Friday', 'Saturday', 'Sunday']

b = weekdays.append('aa')
weekdays.pop(1)
print(weekdays)
weekdays.insert(0, 'Sunday')
a=weekdays
for i in reversed(a):
    print(i)

# with open('C:/Python/leo.txt') as f:
#     for line in f:
#         print(line,end='')




records = [
    ('foo', 1, 2),
    ('bar', 'hello'),
    ('foo', 3, 4),
]

def foo(x,y):
    print('foo: ',x,y)

def bar(x):
    print('bar: ',x)

for tag,*arg in records:
    if tag=='foo':
        foo(*arg)
    if tag=='bar':
        bar(*arg)


from os.path import abspath, realpath,join, dirname
a=join(realpath(dirname(__file__)),'src')
print(a)

add=lambda x,y:x+y
y=add(1,2)
print(y)


def bar(s):
    return 1/int(s)

def main():
    try:
        bar('1')
    except Exception as e:
        print('Error:', e)
    finally:
        print('finally...')


print(int(1))

from datetime import datetime, timedelta
dbtype='json'
a='filepath' if dbtype=='json' else 'source_query'
print(a)
b=' '
path='/opt/json/2019-01-02/a002/2019-01-02/002/000003'
data_format = "%Y-%m-%d"
end_time='2019-01-02'


today_date = (datetime.strptime(end_time, data_format)).strftime("%Y-%m-%d")

if today_date in path:
    print('yes')

else:
    print('none')
print(today_date)
_,path2=path.split(today_date+'/',1)
print(path2)

print(path2.replace('/','_'))
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))+'/embulk_configs/csvfile/mac.yml.liquid'
print(path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': 18,
    'email': 'bi@uuabc.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=60),
    'pool': 'batch_default',
    'priority_weight': 100,
    'catchup': False
}
default_args.update({'start_date': 22})
print(default_args)

import os
filepath = os.path.join('/root', 'macfile.csv')
print(filepath)


import os
path='/miaol/test/mkdir'
if not os.path.exists(path):
    os.makedirs(path)
else:
    print('the file already exists')


a = {
    'x' : 1,
    'y' : 2,
    'z' : 3
}

b = {
    'w' : 10,
    'x' : 11,
    'y' : 2
}

print(type(list(a.values())))
c=a.keys() - b.keys()
print(c)


a=[x*2 for x in range(8) if x%2==0]
for i in a:
    print(i)

dicb={x:y for x,y in b.items()}
print(dicb.values())

print(add(('a',1),('a',2)))

# lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
# counts = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
# output = counts.collect()
# for (word, count) in output:
#     print("%s: %i" % (word, count))

aaaa={1:'aa',2:'bbb',55:'eee',88:'eee'}

bbbb=[1,2,3,4,66]
# b=[a*2 for a in bbbb if a%2==0]
# print(b)




