#Подключаем необходимые пакеты
import findspark
findspark.init()
findspark.find()

import os
import sys
import pyspark
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import when

from utils import get_event_with_city




def calculate_zone_mart(path_event_prqt: str,
                         path_city_data: str,
                         spark: pyspark.sql.SparkSession) ->  pyspark.sql.DataFrame:
    
    df_events_city = get_event_with_city(path_event_prqt = path_event_prqt
                                 ,path_city_data = path_city_data
                                 ,spark = spark)
    
    #Задаем параметры оконных функций
    w2 = Window.partitionBy('user_id', 'event_type')
    w3 = Window.partitionBy('month', 'week','zone_id')
    w4 = Window.partitionBy('month', 'zone_id')

    
    #Рассчитываем витрину
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    zone_mart = df_events_city\
                .select(F.coalesce(F.col('event.datetime'),F.col('event.message_ts')).alias('created_at'),\
                        F.col('event_type'),\
                        F.coalesce(F.col('event.message_from'), F.col('event.user'), F.col('event.reaction_from')).alias('user_id'),                                 F.col('city_id').alias('zone_id'))\
                .withColumn('created_at', F.to_timestamp(F.col('created_at')))\
                .withColumn('first_message_date', F.min('created_at').over(w2))\
                .withColumn('is_registration', when((F.col('event_type') == "message") & (F.col('created_at') ==                      F.col('first_message_date')),1)
                                               .otherwise(0))\
                .withColumn('month', F.trunc(F.col("created_at"), "month"))\
                .withColumn('week' ,F.date_format(F.to_date(F.col("created_at"), "dd/MMM/yyyy"), "W"))\
                .withColumn('week_message', F.count(when(F.col("event_type") == "message", True)).over(w3))\
                .withColumn('week_reaction', F.count(when(F.col("event_type") == "reaction", True)).over(w3))\
                .withColumn('week_subscription', F.count(when(F.col("event_type") == "subscription", True)).over(w3))\
                .withColumn('week_user', F.count(when(F.col("is_registration") == 1, True)).over(w3))\
                .withColumn('month_message', F.count(when(F.col("event_type") == "message", True)).over(w4))\
                .withColumn('month_reaction', F.count(when(F.col("event_type") == "reaction", True)).over(w4))\
                .withColumn('month_subscription', F.count(when(F.col("event_type") == "subscription", True)).over(w4))\
                .withColumn('month_user', F.count(when(F.col("is_registration") == 1, True)).over(w4))\
                .select(F.col('month'), F.col('week'), F.col('zone_id'), F.col('week_message'), F.col('week_reaction'),F.col('week_subscription'), F.col('week_user'),F.col('month_message'),F.col('month_reaction'), F.col('month_subscription'),F.col('month_user'))\
                .dropDuplicates()

    
    return zone_mart


def main():
    spark = SparkSession \
    .builder \
    .master("yarn") \
        .config("spark.driver.cores", "2") \
        .config("spark.driver.memory", "2g") \
        .appName("project_zhukovanan_new") \
        .getOrCreate()

    path_event_prqt = sys.argv[1]
    path_city_data = sys.argv[2]
    path_mart_to_save = sys.argv[3]
    calculate_zone_mart(path_event_prqt,path_city_data,spark)\
        .write.format('json').mode('overwrite').save(f'{path_mart_to_save}/zone_mart.json')


if __name__ == '__main__':
    main()

    