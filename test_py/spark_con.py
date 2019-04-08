from pyhive import hive


def fetch(sql):
    cursor = hive.connect('10.68.100.69').cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    return results

datas=fetch('''
select uuid
,`name`
,province
,city
,signup_time
,phone
,email
,channel
,gender
,year_of_birth
,birthday
,first_pay_date
,user_type
,is_test_account
,is_paid
,cc
,cr
from tmp.user_json limit 10''')
r=[]
j=0
for i in ["uuid","name","province","city","signup_time","phone","email","channel","gender","year_of_birth","birthday",
          "first_pay_date","user_type","is_test_account","is_paid","cc","cr"]:
   r.append(i)
   j+=1

for line in datas:
    uuid, name, province, city, signup_time, phone, email, channel, gender, year_of_birth, birthday, first_pay_date, user_type, is_test_account, is_paid, cc, cr=line
    json_line='{"' + r[0] + '":' + str(uuid) + ',"' + r[1] + '":"' + str(b) + '","' + c_name + '":"' + str(c) + '"}'

