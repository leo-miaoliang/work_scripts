
def Fb_list(m):

    a, b, n = 0, 1, 1
    while n<m:
        a,b=b,a+b
        n=n+1
        yield b


# f=Fb_list(6)
# for i in f:
#     print(i)


_mysql_to_spark_type_mapping = {
    'bigint': 'bigint',
    'int': 'bigint',
    'mediumint': 'bigint',
    'smallint': 'bigint',
    'tinyint': 'bigint',
    'float': 'double',
    'decimal': 'double',
    'char': 'string',
    'varchar': 'string',
    'datetime': 'string',
    'date': 'string',
    'timestamp': 'string',
    'text': 'string',
    'mediumtext': 'string',
    'enum': 'string',
    'longtext': 'string'
}



_presto_to_spark_type_mapping = {
    'double': 'double',
    'boolean': 'string',
    'bigint': 'bigint',
    'decimal(,)': 'double',
    'decimal()': 'double',
    'integer': 'bigint',
    'timestamp with time zone': 'string',
    'varchar': 'string',
    'varchar()': 'string',
    'tinyint': 'bigint',
    'time': 'string',
    'date': 'string',
    'smallint': 'bigint',
    'timestamp': 'string'
}

def del_digits(x):
    for i in range(10):
        x=x.replace(str(i),'')
    return x
def convert_data_type(data_type):
    data_type=del_digits(data_type)
    if data_type in _presto_to_spark_type_mapping:
        return _mysql_to_spark_type_mapping[data_type]

if 'stri' in _mysql_to_spark_type_mapping:
    print('hello')


a=del_digits('decimal(38,5)')
print(a)

#
# import numpy as np
#
# X = np.arange(-1.0, 1.0, 0.1).reshape(10,2)
# print(X)
# for line in X:
#     a,b=line
#     print(a,'---',b)
#
# f = open("C:/Users/Miaol/Desktop/a.txt")
# line = f.readlines()
# print(type(line))
# while line:
#     print(line)
#     line = f.readline()
# f.close()

import string
for letter in string.printable:
    print(letter)