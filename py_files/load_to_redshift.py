import psycopg2
import boto3
import configparser

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
#get redshift database credential
redshift_dbname = parser.get("aws_redshift_creds","database")
redshift_host = parser.get("aws_redshift_creds","host")
redshift_port = parser.get("aws_redshift_creds","port")
redshift_username = parser.get("aws_redshift_creds","username")
redshift_password = parser.get("aws_redshift_creds","password")

string = "dbname="+redshift_dbname+" user="+redshift_username+" password="+redshift_password+" host="+redshift_host+" port="+redshift_port
redshift_con = psycopg2.connect(string)
if redshift_con:
    print("Redshift Connection Established")
else:
    print("Redshift Connection Error")    
    os.exit()

redshift_cursor = redshift_con.cursor()

#
s3_account_id = parser.get("aws_boto_credentials","account_id")
s3_bucket_name = parser.get("aws_boto_credentials","bucket_name")
redshift_iam_role = parser.get("aws_redshift_creds","iam_role")

#copy
file_path = "s3://"+s3_bucket_name+"/order_incremental_extract.csv"
role_string = "arn:aws:iam::"+s3_account_id+":role/"+redshift_iam_role

sql = "copy public.Orders"
sql += " from '%s' iam_role '%s'"%(file_path,role_string)
sql += " delimiter ','"

redshift_cursor.execute(sql)
redshift_con.commit()

redshift_cursor.close()
redshift_con.close()


