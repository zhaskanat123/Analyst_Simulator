from datetime import datetime, timedelta 
import pandas as pd
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

def ch_get_df(query = 'SELECT 1', host = 'https://clickhouse.lab.karpov.courses', user = 'student', password = 'dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

default_args = {
    'owner': 'z-shaimurat',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 7, 23),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_lesson_7_ex_1():
    
    @task()
    def extract_feed_actions():
        query = """SELECT user_id, 
                           toDate(time) as event_date,
                           gender,
                           os, 
                           age,
                           countIf(action='like') AS likes, 
                           countIf(action='view') AS views
                    FROM simulator_20240620.feed_actions
                    WHERE toDate(time) = today() - 1
                    GROUP BY user_id, gender, os, age, event_date
                    format TSVWithNames"""
        df_feed_actions = ch_get_df(query=query)
        return df_feed_actions
    
    @task()
    def extract_message_actions():
        query = """SELECT
                        user_id,
                        toDate(time) as event_date,
                        gender,
                        os,
                        age,
                        COUNT(*) AS messages_sent,
                        COUNT(DISTINCT receiver_id) AS users_received,
                        received.messages_received,
                        received.users_sent
                    FROM simulator_20240620.message_actions 
                        LEFT JOIN
                        (
                            SELECT
                                receiver_id,
                                toDate(time) as event_date,
                                COUNT(*) AS messages_received,
                                COUNT(DISTINCT user_id) AS users_sent
                            FROM simulator_20240620.message_actions
                            WHERE toDate(time) = today() - 1
                            GROUP BY receiver_id, event_date
                        ) AS received
                        ON
                            message_actions.user_id = received.receiver_id
                        WHERE toDate(time) = today() - 1
                        GROUP BY user_id, received.messages_received, received.users_sent, event_date, gender, os, age
                        ORDER BY user_id"
                        format TSVWithNames"""
        df_message_actions = ch_get_df(query=query)
        return df_message_actions    
    
    @task()
    def transform_unite_tables(df_feed_actions, df_message_actions):
        merged_df = pd.merge(df_feed_actions, df_message_actions, on=['user_id', 'event_date', 'gender', 'os', 'age'], how='inner')
        return merged_df
    
    df_feed_actions = extract_feed_actions()
    df_message_actions = extract_message_actions()
    df_merged = transform_unite_tables(df_feed_actions, df_message_actions)
    
dag_lesson_7_ex_1 = dag_lesson_7_ex_1()  
    

        
        
        
        
        
        
    