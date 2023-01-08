from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2020, 1, 1),
                                }

dag_spark = DAG(
                        dag_id = "Project",
                        default_args=default_args,
                        schedule_interval="@daily",
                        )

# объявляем задачу с помощью SparkSubmitOperator
calculate_user_mart = SparkSubmitOperator(
                        task_id='calculate_user_mart',
                        dag=dag_spark,
                        application ='/scripts/calculate_user_mart.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["/user/master/data/geo/events", "/user/zhukovanan/data/geo.csv"       
                                            ,"/user/zhukovanan/analytics"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

calculate_zone_activity_mart = SparkSubmitOperator(
                        task_id='calculate_zone_activity_mart',
                        dag=dag_spark,
                        application ='/scripts/calculate_zone_activity_mart.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["/user/master/data/geo/events", "/user/zhukovanan/data/geo.csv"       
                                            ,"/user/zhukovanan/analytics"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

calculate_user_rec_mart = SparkSubmitOperator(
                        task_id='calculate_user_rec_mart',
                        dag=dag_spark,
                        application ='/scripts/calculate_user_rec_mart.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["/user/master/data/geo/events", "/user/zhukovanan/data/geo.csv"       
                                            ,"/user/zhukovanan/analytics"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )
(calculate_user_mart >>
 calculate_zone_activity_mart >>
 calculate_user_rec_mart)