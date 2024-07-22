#import the DAG object
from airflow import DAG
#import the operators used in tasks
from airflow.operators.python_operator import PythonOperator
#import the days_ago function
from airflow.utils.dates import days_ago
import pandas  as pd
# for loading and processing the data

import os
#get DAG directory path
'''
we keep track of the path in which the DAG is located
this path will be different when it is executed inside the airflow environment
it is also different when u r testing on a local machine
'''
dag_path=os.getcwd()
'''
low_memory=False: Pandas reads the entire file into memory at once. 
This allows it to infer the data types of columns more accurately but 
can consume a significant amount of memory for large files.

'''
def transform_data():
    booking=pd.read_csv(f'{dag_path}/raw_data/booking.csv',low_memory=False)
    client=pd.read_csv(f'{dag_path}/raw_data/booking.csv',low_memory=False)
    client=pd.read_csv(f'{dag_path}/raw_data/booking.csv',low_memory=False)

    #merging csv files on the basis of IDs

    #1stly merge booking & client
    data=pd.merge(booking,client, on='client_id')
    data.rename(columns={'name':'client_name','type':'client_type'}, inplace=True)

    #2ndly merge data(booking & client) with hotel

    data=pd.merge(data,hotel,on='hotel_id')
    data.rename(columns={'name':'hotel_name'},inplace=True)

    #make date format consistent
    data.booking_date=pd.to_datetime(data.booking_date,infer_datetime_format=True)
    #make all cost in GBP currency
    data.loc[data.currency=='EUR',['booking_cost']]=data.booking_cost*0.8
    data.currency.replace('EUR','GBP',inplace=True)

    #remove unnecessary columns
    data=data.drop('address',1)
    #load processed data
    data.to_csv(f'{dag_path}/processed_data/processed_data.csv',index=False)
    '''
    index=False, means the row indices are not included in the CSV file
    '''

def load_data():
    conn=sqlite3.connect('/usr/local/airflow/db/datascience.db')
    c=conn.cursor()
    c.execute('''
              CREATE TABLE IF NOT EXISTS booking_record(
                client_id integer not,
                booking_date text not null,
                room_type TEXT(512) not null,
                hotel_id integer not null,
                booking_cost numeric,
                currency text,
                age integer,
                client_name text(512),
                client_type text(512),
                hotel_name text(512)
              );
              ''')
    records=pd.read_csv(f'{dag_path}/processed_data/processed_data.csv')
    records.to_sql('b')


#1st building block
#initializing the dedault arguments that we will pass to our DAG
default_args={
    'owner':'airflow',
    'start_date':days_ago(5) # time for the trigger
}

#definning the dag itself

ingestion_dag=DAG(
    'booking_ingestion',
    default_args=default_args,
    description='Aggregates booking records for data analysis', # description of the dag
    schedule_interval=timedelta(days=1), #tells the airflow when u need to trigger this tag, here it is (everyday midnight_
    catchup=False #if the catchup is true then airflow is going to start this tag from 5 fays before

)
#our first task is an instance of python operator named:"transofrm_data"
task_1=PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=ingestion_dag,
)
# an instance of python operator called:"load data"
task_2=PythonOperator(
    task_id='load_data',
    python_callable=load_data
    dag=ingestion_dag,
)


#last building block = define the dependencies
task_1>>task_2