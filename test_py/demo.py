# -*- coding: utf-8 -*-


a='mysql_hive_newuuabc.aperison_vlue'
b=a.split('_',2)
print("path_prefix%s:'%s' " ,a,b)
aaa_list=[a]
#aaa_list.append('aa')
print(type(aaa_list))
b=a.split('_',2)
print(b)

import heapq
a= [1, 8, 2, 23, 7, -4, 18, 23, 42, 37, 2]
b1=heapq.nlargest(3,a)
b2=heapq.nsmallest(3,a)
print(b1,'\n',b2)

#-------example 1---------------------
# with open('C:/Users/Miaol/Desktop/配置信息.txt') as f:
#     while True:
#         line = next(f, None)
#         if line is None:
#             break
#         print(line)


#--------example 2-----------------------

# with open('C:/Users/Miaol/Desktop/配置信息.txt') as f:
#     i=0
#     try:
#         while i<10:
#             line=next(f)
#             i+=1
#             print(line,end='')
#     except StopIteration:
#         print("it's the end")


# from settings import  select_query
# a=select_query('select dbtype from etl.dbsyn_dblink')
# print(a)

aa=[('mysql',), ('jdbc',), ('jdbc',), ('mysql',)]
print(aa[3][0])


from datetime import datetime,timedelta
start_date=datetime(2018,10,23,14,1)
print(start_date)


date1=datetime.utcnow()
date2=datetime.now()

aaa='20180101'
bbb=(datetime.strptime(aaa, '%Y%m%d' )-timedelta(days = 1)).strftime('%Y-%m-%d')
print(bbb)

bbbb=len(aaa)
print(bbbb)


#print('date1:',date1,' date2:',date2)

print("%s the old '%s'" %(aaa,bbbb))

a={1,2}
print(a)
print(type(a))


bb='hello'
print(dir(bb))

print('###########')
# ff=lambda x: True if x==2 else False
# print(ff(2))


r=[]
j=0
for i in ["uuid","name","province","city","signup_time","phone","email","channel","gender","year_of_birth","birthday",
          "first_pay_date","user_type","is_test_account","is_paid","cc","cr"]:
   r.append(i)
   print(j)
   print(r[j])
   j+=1

