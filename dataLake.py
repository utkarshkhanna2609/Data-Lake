from datetime import datetime
import ntpath
from platform import python_implementation
from sre_constants import SUCCESS
from airflow.utils.dates import days_ago
from airflow import DAG
from requests import request
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import csv
from datetime import datetime
from email import header
from email.charset import Charset
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
import csv,os.path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import pandas as pd
import logging
import boto3
from botocore.exceptions import ClientError
import os

filepath='/home/ubuntu/Utkarsh/'
figma_schema="export_comments"

args = {
    'owner': 'Utkarsh',
    'start_date': days_ago(1),
}

def start():
    return "start"

def get_tables():
    request="SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA ='"+figma_schema+"'"
    mysql_hook = MySqlHook(mysql_conn_id ='figma-plugin-export')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()    #ask purpose
    cursor.execute(request)     
    sources = cursor.fetchall()    
    tables=[]
    for elements in sources:
        tables.append(elements)
    return tables

def get_columns(request,file):
  mysql_hook = MySqlHook(mysql_conn_id ='figma-plugin-export')
  connection = mysql_hook.get_conn()
  cursor = connection.cursor()
  cursor.execute(request)
  sources = cursor.fetchall()
  file=ospath.join(filepath,file)
  csvfile=open(file,'w', newline='')
  obj=csv.writer(csvfile)
  for row in sources:
    obj.writerow(row)
  csvfile.close()

  
def transpose(file):
  pd.read_csv(file, header=None).T.to_csv(file,header=False,index=False)
  


def get_records(query,file):
  mysql_hook = MySqlHook(mysql_conn_id ='figma-plugin')
  connection = mysql_hook.get_conn()
  cursor = connection.cursor()
  cursor.execute(query)
  sources = cursor.fetchall()
  file=ntpath.join(filepath,file)
  transpose(file)          # Yet to write the function                                     
  csvfile=open(file,'a', newline='')   #taken in append mode
  obj=csv.writer(csvfile)
  for row in sources:
    obj.writerow(row)
  csvfile.close()


def export_csv():
  for tables in get_tables():
    tables=str(tables[0])
    request="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA ='"+figma_schema+"' AND TABLE_NAME = '"+tables+"'"
    file_name=tables+".csv"
    get_columns(request,file_name)
    query="SELECT * FROM "+tables
    get_records(query,file_name)




def upload_to_lake(tables,file_path):
    s3_hook = S3Hook(aws_conn_id='dvc-pipeline')
    folder='Utkarsh'
    s3_key=folder+'/'+tables+'/'+tables+'.csv'
   
    s3_hook.load_file(file_path, bucket_name='intelligaia-datalake', key=s3_key,replace=True)
    
def run_dag():
    for tables in get_tables():
        table_name=str(tables[0])
        filename=table_name+".csv"
        file_path=os.path.join(filepath,filename)
        results=pd.read_csv(file_path)
        if len(results)>0:
          upload_to_intelake(table_name,file_path)
        else:
          return 'Cannot Move Data'+filename+', has 0 records'
   

#dags



dag=DAG(dag_id='utkarsh',
schedule_interval='@daily',
start_date=datetime(2022,1,10),
catchup=False,
default_args=args,
tags=['Training']

)


start=PythonOperator(
    task_id="Start",
    python_callable=start,
    dag=dag)

getTables=PythonOperator(
    task_id="Getting_record_of_Tables",
    python_callable=get_tables,
    dag=dag
)

getColumns=DummyOperator(
    task_id='Getting_Columns_from_tables',  
    dag=dag
)

transposeFun=DummyOperator(  #Taking the transpose of table names stored in columns
    task_id='Transpose',
    dag=dag
)

getRecords=DummyOperator(
    task_id='Sql_Hook',
    dag=dag
)

exportFile=PythonOperator(
    task_id='Exporting_files', #using for loop, we iterate through the different tables, use functions get_columns and get_records
    python_callable=export_csv,
    dag=dag
)

upload=PythonOperator(
    task_id='Uploading_files', #using for loop, we iterate through the different tables, use functions get_columns and get_records
    python_callable=run_dag,
    dag=dag
)





start>>getTables>>getColumns>>transposeFun>>getRecords>>exportFile>>upload











