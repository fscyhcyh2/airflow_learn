import pymysql
import csv
import boto3
import configparser

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config","hostname")
port = parser.get("mysql_config","port")
username = parser.get("mysql_config","username")
dbname = parser.get("mysql_config","database")
password = parser.get("mysql_config","password")

con = pymysql.connect(host=hostname,user=username,password=password,db=dbname,port=int(port))

if con is None:
    print("Error connecting to MySQL")
else:
    print("MySQL connection established")    

order_query = "select * from Orders;"
local_filename = "order_full_extract.csv"

mysql_cursor = con.cursor()
mysql_cursor.execute(order_query)
result = mysql_cursor.fetchall()

print(type(result))
with open(local_filename,'w') as local_file:
    csv_write = csv.writer(local_file,delimiter=',')
    csv_write.writerows(result)

local_file.close()
mysql_cursor.close()
con.close()

import load_to_s3
load_to_s3.load_to_s3(local_filename)
