from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import os
import pandas as pd
from elasticsearch import Elasticsearch

def get_csv_files(directory):
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    dataframes = []
    for file in csv_files:
        file_path = os.path.join(directory, file)
        df = pd.read_csv(file_path, dtype={'designation': 'str', 'region_1': 'str'})  
        dataframes.append(df)
        print(f"Файл {file} загружен, размер: {df.shape}")
    combined_df = pd.concat(dataframes, ignore_index=True)
    print(f"Все файлы объединены. Размер итогового DataFrame: {combined_df.shape}")
    combined_df.to_csv('All.csv', index=False)
    

def deletes_null():
    combined_df = pd.read_csv('All.csv')
    combined_df = combined_df.dropna(subset=['designation', 'region_1'])
    print(f"Файл почищен. Размер DataFrame: {combined_df.shape}")
    combined_df.to_csv('All.csv', index=False)

def update_price():
    combined_df = pd.read_csv('All.csv')
    combined_df = combined_df.fillna({'price': 0.0})
    print("Price заменен")
    combined_df.to_csv('All.csv', index=False)

def save_csv(directory):
     combined_df = pd.read_csv('All.csv')
     output_file_path = os.path.join(directory, 'out.csv')
     combined_df.to_csv(output_file_path, index=False)
     print("Сохранен")

def save_elastic():
    combined_df = pd.read_csv('All.csv')
    elastic_search = Elasticsearch("http://elasticsearch-kibana:9200")
    for _, row in combined_df.iterrows():
            elastic_search.index(index='wine_data_2', body= row.to_json())

parent_directory_input = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'lr1', 'input')  
parent_directory_output = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'lr1', 'output')  

with DAG(
    dag_id="lr24",
    schedule="0 0 * * *", 
    start_date=datetime.datetime(2024, 10, 22, tzinfo=datetime.timezone.utc), 
    catchup=False,  
    dagrun_timeout=datetime.timedelta(minutes=1),  
    tags=["LR"],
) as dag:

    task1 = PythonOperator(
        task_id='get_csv_files',  
        python_callable=get_csv_files,  
        op_args=[parent_directory_input],
        provide_context=True  
    )

    task2 = PythonOperator(
        task_id='deletes_null',
        python_callable = deletes_null,
        provide_context=True
    )

    task3 = PythonOperator(
        task_id = 'update_price',
        python_callable=update_price,
        provide_context=True
    )

    task4 = PythonOperator(
        task_id = 'save_csv',
        python_callable=save_csv,  
        op_args=[parent_directory_output],
        provide_context=True  
    )
    task5 = PythonOperator(
        task_id = 'save_elastic',
        python_callable=save_elastic,
        provide_context=True  
    )

    task1 >> task2 >> task3 >> [task4, task5]
