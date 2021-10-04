import pymysql
import csv
import configparser
import psycopg2
import os

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
#get redshift database credential
redshift_dbname = parser.get("aws_redshift_creds","database")
redshift_host = parser.get("aws_redshift_creds","host")
redshift_port = parser.get("aws_redshift_creds","port")
redshift_username = parser.get("aws_redshift_creds","username")
redshift_password = parser.get("aws_redshift_creds","password")

#connect to redshift to get the last udpate time
string = "dbname="+redshift_dbname+" user="+redshift_username+" password="+redshift_password+" host="+redshift_host+" port="+redshift_port

redshift_con = psycopg2.connect(string)

if redshift_con:
    print("Redshift Connection Established")
else:
    print("Redshift Connection Error")    
    os.exit()

redshift_cursor = redshift_con.cursor()
redshift_getLastUpdate_sql = \
"select nvl(max(lastupdated),'1900-01-01') from orders"
redshift_cursor.execute(redshift_getLastUpdate_sql)
last_update_date = redshift_cursor.fetchone()[0]

redshift_cursor.close()
redshift_con.close()


#connect to source database

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



#get new data from source
order_query = "select * from Orders where lastupdated > '%s';"%(last_update_date)
local_filename = "/home/fscyhcyh/Documents/de_pocket_project/pipelines/py_files/order_incremental_extract.csv"
print(order_query)
mysql_cursor = con.cursor()
mysql_cursor.execute(order_query)
result = mysql_cursor.fetchall()

print(result)
with open(local_filename,'w') as local_file:
    csv_write = csv.writer(local_file,delimiter=',')
    csv_write.writerows(result)

local_file.close()
mysql_cursor.close()
con.close()

import load_to_s3
load_to_s3.load_to_s3(local_filename)
