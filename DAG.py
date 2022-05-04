#!/usr/bin/env python
# coding: utf-8

# In[1]:


from influxdb_client import Point, InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
from influxdb import DataFrameClient
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.influxdb.operators.influxdb import InfluxDBOperator
from airflow.operators.bash import BashOperator


# In[2]:


default_args = {'owner' : 'admin'}


# In[3]:


dag = DAG('study-case' , default_args = default_args , start_date = datetime (2022,1,1) , tags = ['case' , 'IDSA'] , catchup = False , schedule_interval = '*/10 * * * *')


# In[4]:


def task1():    
    bucket = "telegraf"
    org = "ege"
    token = "O-kL5PlzpV_vQbY7L0x-bgOD_0izVPxql3nHWpKzWqRLPPsiKDo9g6DxL_VP03AZq-AiAPfbHYv6UTeO7RuSrw=="
    url = "http://34.69.19.183:8086"
    
    query = 'import "math"\
    import "contrib/anaisdg/statmodels"\
    original = from(bucket: "telegraf")\
      |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\
      |> filter(fn: (r) => r["_measurement"] == "cpu")\
      |> filter(fn: (r) => r["_field"] == "usage_user")\
      |> filter(fn: (r) => r["cpu"] == "cpu0")\
      |> filter(fn: (r) => r["host"] == "influxdb")\
      |> map(fn: (r) => ({r with _value r//100}))\
      |> aggregateWindow(every: 10m, fn: mean, createEmpty: false)\
      |> yield(name: "last")'
    
    client = InfluxDBClient(url = url , token = token , org = org , debug = False)
    df = client.query_api().query_date_frame(org=org , query = query)
    df = df.set_index("_time")
    _write_client.write(bucket , record =df , data_frame_measurement_name='case-test' , data_frame_tag_columns=['host' , '_field'])
    _write_client.__del__()
    client.__del__()


# In[5]:


def task2():    
    bucket = "telegraf"
    org = "ege"
    token = "O-kL5PlzpV_vQbY7L0x-bgOD_0izVPxql3nHWpKzWqRLPPsiKDo9g6DxL_VP03AZq-AiAPfbHYv6UTeO7RuSrw=="
    url = "http://34.69.19.183:8086"
    
    query = 'import "math"\
    import "contrib/anaisdg/statmodels"\
    original = from(bucket: "telegraf")\
      |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\
      |> filter(fn: (r) => r["_measurement"] == "cpu")\
      |> filter(fn: (r) => r["_field"] == "usage_user")\
      |> filter(fn: (r) => r["cpu"] == "cpu1")\
      |> filter(fn: (r) => r["host"] == "influxdb")\
      |> map(fn: (r) => ({r with _value r//100}))\
      |> aggregateWindow(every: 10m, fn: mean, createEmpty: false)\
      |> yield(name: "last")'
    
    client = InfluxDBClient(url = url , token = token , org = org , debug = False)
    df = client.query_api().query_date_frame(org=org , query = query)
    df = df.set_index("_time")
    _write_client.write(bucket , record =df , data_frame_measurement_name='case-test' , data_frame_tag_columns=['host' , '_field'])
    _write_client.__del__()
    client.__del__()


# In[6]:


def task3():    
    bucket = "telegraf"
    org = "ege"
    token = "O-kL5PlzpV_vQbY7L0x-bgOD_0izVPxql3nHWpKzWqRLPPsiKDo9g6DxL_VP03AZq-AiAPfbHYv6UTeO7RuSrw=="
    url = "http://34.69.19.183:8086"
    
    query = 'import "math"\
    import "contrib/anaisdg/statmodels"\
    original = from(bucket: "telegraf")\
      |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\
      |> filter(fn: (r) => r["_measurement"] == "disk")\
      |> filter(fn: (r) => r["_field"] == "total" or r["_field"] == "free")\
      |> filter(fn: (r) => r["device"] == "sda1")\
      |> filter(fn: (r) => r["fstype"] == "ext4")\
      |> filter(fn: (r) => r["host"] == "influxdb")\
      |> map(fn: (r) => ({free // total}))\
      |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)\
      |> yield(name: "last")'
    
    client = InfluxDBClient(url = url , token = token , org = org , debug = False)
    df = client.query_api().query_date_frame(org=org , query = query)
    df = df.set_index("_time")
    _write_client.write(bucket , record =df , data_frame_measurement_name='case-test' , data_frame_tag_columns=['host' , '_field'])
    _write_client.__del__()
    client.__del__()


# In[7]:


def task4():    
    bucket = "telegraf"
    org = "ege"
    token = "O-kL5PlzpV_vQbY7L0x-bgOD_0izVPxql3nHWpKzWqRLPPsiKDo9g6DxL_VP03AZq-AiAPfbHYv6UTeO7RuSrw=="
    url = "http://34.69.19.183:8086"
    
    query = 'import "math"\
    import "contrib/anaisdg/statmodels"\
    original = from(bucket: "telegraf")\
      |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\
      |> filter(fn: (r) => r["_measurement"] == "mem")\
      |> filter(fn: (r) => r["_field"] == "used" or r["_field"] == "total")\
      |> filter(fn: (r) => r["host"] == "influxdb")\
        |> map(fn: (r) => ({free // total}))\
      |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)\
      |> yield(name: "mean")'
    
    client = InfluxDBClient(url = url , token = token , org = org , debug = False)
    df = client.query_api().query_date_frame(org=org , query = query)
    df = df.set_index("_time")
    _write_client.write(bucket , record =df , data_frame_measurement_name='case-test' , data_frame_tag_columns=['host' , '_field'])
    _write_client.__del__()
    client.__del__()


# In[8]:


with dag:
    task1 = PythonOperator(
    task_id = 'task1',
    python_callable = task1,
    )
    
    task2 = PythonOperator(
    task_id = 'task2',
    python_callable = task2,
    )
    
    task3 = PythonOperator(
    task_id = 'task3',
    python_callable = task3,
    )
    
    task4 = PythonOperator(
    task_id = 'task4',
    python_callable = task4,
    )
    
    bash_message = BashOperator(
    task_id='bash_message',
    bash_command='echo "Tüm tasklar tamamlandı"; exit 99;')


task1 >> task2 >> [task3 , task4] >> bash_message 
    
    
        

