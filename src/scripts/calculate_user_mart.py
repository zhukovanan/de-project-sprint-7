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




def calculate_user_marts(path_event_prqt: str,
                         path_city_data: str,
                         spark: pyspark.sql.SparkSession) ->  pyspark.sql.DataFrame:
    
    df_events_city = get_event_with_city(path_event_prqt = path_event_prqt
                                 ,path_city_data = path_city_data
                                 ,spark = spark)

    
    #Подготавливаем данные для витрины в разрезе пользователей, размеченные по отправленным сообщениям

    user_data = df_events_city\
            .where("event_type=='message' AND event.message_ts is not null")\
            .withColumn("user_id", F.col("event.message_from"))\
            .select("user_id","city", "event.message_ts", "city_id", "event.message_to", 'city_lat', 'city_lng')


    #Находим активный город пользователя, т.е. город из которого было отправлено последнее сообщение и сразу размечаем 
    #локальное время последнего события, для определенных городов в ручную задаем временную зону - потому что для них не находит время

    w_act_city = Window.partitionBy('user_id') 

    user_act_city = user_data\
                .withColumn('last_message_ts', F.max('message_ts').over(w_act_city))\
                .where(F.col('message_ts') == F.col('last_message_ts'))\
                .select('user_id', 'city', 'message_ts')\
                .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('city')))\
                .withColumn('timezone', F.regexp_replace('timezone', "(Bunbury)", "Perth"))\
                .withColumn('timezone', F.regexp_replace('timezone', "(Mackay)|(Wollongong)|(Toowoomba)|(Townsville)|(Cairns)|(Rockhampton)|(Gold Coast)|(Ipswich)", "Brisbane"))\
                .withColumn('timezone', F.regexp_replace('timezone', "(Cranbourne)|(Geelong)|(Newcastle)|(Bendigo)|(Launceston)|(Ballarat)|(Maitland)|(Hobart)", "Sydney"))\
                .withColumn('local_time',F.from_utc_timestamp(F.col("message_ts"),F.col('timezone')))



    #Находим город из которого было отправлено сообщения не менее 27 дней подряд, то есть необходимо найти непрерывную
    #последовательность отправок сообщений, длина которой не менее 27-ми дней

    w_home_city = Window.partitionBy('user_id', 'city').orderBy('message_ts') 

    user_home_city = user_data \
                .select('user_id', 'city', 'message_ts')\
                .withColumn('message_ts', F.to_date('message_ts'))\
                .dropDuplicates()\
                .withColumn('rank', F.row_number().over(w_home_city))\
                .withColumn('group', F.expr("date_add(message_ts, -rank)"))\
                .groupBy('user_id', 'city', 'group')\
                .count()\
                .where(F.col('count') >= 27)\
                .select('user_id' , 'city')


    #Составляет списки город по посещению и подсчитываем визиты

    w_visit_city = Window.partitionBy('user_id').orderBy('message_ts') 

    user_city_visits = user_data\
                    .withColumn('before_city', F.lag('city').over(w_visit_city))\
                    .withColumn('group', when(F.col('before_city').isNull(),1)
                                        .when(F.col('before_city') == F.col('city'),0)
                                        .otherwise(1))\
                    .where(F.col("group")== 1)\
                    .groupBy('user_id')\
                    .agg(F.sort_array(F.collect_list(F.struct("message_ts", "city"))).alias("collected_list"), F.count("*").alias('travel_count'))\
                    .withColumn("travel_array",F.col("collected_list.city"))\
                    .drop("collected_list")


    #Собираем итоговую витрину, если home city не удалось определить, то в качестве него берем act_city и сохраняем
    return user_act_city.alias('a')\
            .join(user_home_city.alias('b'), F.col('a.user_id') == F.col('b.user_id'),how='left')\
            .join(user_city_visits.alias('v'),F.col('a.user_id') == F.col('v.user_id'),how='left')\
            .select(F.col('a.user_id'),F.col('a.city').alias('act_city'), F.coalesce(F.col('b.city'),F.col('a.city')).alias('home_city'),F.col('v.travel_count') ,F.col('v.travel_array'), F.col('local_time'))


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
    calculate_user_marts(path_event_prqt,path_city_data,spark)\
        .write.format('json').mode('overwrite').save(f'{path_mart_to_save}/user_mart.json')


if __name__ == '__main__':
    main()

    