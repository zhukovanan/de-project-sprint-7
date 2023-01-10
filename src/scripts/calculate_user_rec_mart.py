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




def calculate_user_rec_mart(path_event_prqt: str,
                         path_city_data: str,
                         spark: pyspark.sql.SparkSession) ->  pyspark.sql.DataFrame:
    
    df_events_city = get_event_with_city(path_event_prqt = path_event_prqt
                                 ,path_city_data = path_city_data
                                 ,spark = spark)

    #Подгтотавливаем параметры окон и данные для витрины 
    window = Window().partitionBy('user').orderBy('date').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    window_d = Window().partitionBy('user')
    df_city_from = df_events_city.selectExpr('event.message_from as user','city_id', 'city','city_lat','city_lng', 'event.message_ts AS date')
    df_city_to = df_events_city.selectExpr('event.message_to as user', 'city_id', 'city','city_lat', 'city_lng', 'event.message_ts AS date')

    #Находим откуда последний раз пользователи взаимодействовали
    df = df_city_from.union(df_city_to)\
                        .where('date is not null')\
                        .select(F.col('user'),F.col('date'),F.col('city'),F.col('city_id'),F.col('city_lat'),F.col('city_lng'))\
                        .withColumn('rank', F.row_number().over(Window.partitionBy('user_id').orderBy(F.desc('date'))))\
                        .filter('rank < 2')\
                        .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('city')))\
                        .withColumn('timezone', F.regexp_replace('timezone', "(Bunbury)", "Perth"))\
                        .withColumn('timezone', F.regexp_replace('timezone', "(Mackay)|(Wollongong)|(Toowoomba)|(Townsville)|(Cairns)|(Rockhampton)|(Gold Coast)|(Ipswich)", "Brisbane"))\
                        .withColumn('timezone', F.regexp_replace('timezone', "(Cranbourne)|(Geelong)|(Newcastle)|(Bendigo)|(Launceston)|(Ballarat)|(Maitland)|(Hobart)", "Sydney"))\
                        .withColumn('local_time',F.from_utc_timestamp(F.col("date"),F.col('timezone')))

    #Строим рекомендации по расстоянию
    df_mart = df.alias('df_friends').crossJoin(df.alias('df'))\
            .withColumn('dif', F.acos(F.sin(F.col('df_friends.lat_to'))*F.sin(F.col('df.lat_to')) + F.cos(F.col('df_friends.lat_to'))*F.cos(F.col('df.lat_to'))*F.cos(F.col('df_friends.lng_to')-F.col('df.lng_to')))*F.lit(6371))\
            .filter(F.col('dif')<=1)\
            .withColumn("user_left", when(F.col("df_friends.user") > F.col("df.user"), F.col("df_friends.user"))\
             .otherwise(F.col("df.user")))\
             .withColumn("user_right", when(F.col("df_friends.user") > F.col("df.user"), F.col("df.user"))\
             .otherwise(F.col("df_friends.user")))\
             .select("user_left", "user_right", 'df_friends.city_id', 'df_friends.local_time')\
             .distinct()
    
    
    #Исключаем тех кто переписывался
    messages = df_events_city.where("event_type='message' and event.message_from is not null and event.message_to is not null")\
                 .select(F.col("event.message_from").alias("message_from"), F.col("event.message_to").alias("message_to"))\
                 .distinct()
    contacted_users = messages.union(messages.select("message_to", "message_from")).distinct()


    no_concact_users = df_mart.join(contacted_users, [F.col("user_left") == F.col("message_to"), F.col("user_right") == F.col("message_from")], "leftanti")
             
    #Соединяем с тем, кто подписан на один канал
    user_subscription = df_events_city.where("event_type='subscription'")\
                        .select(F.col("event.subscription_channel").alias("subscription_channel"),\
                        F.col("event.user").alias("subscription_user"))\
                        .distinct()

    return no_concact_users.alias("p1").join[user_subscription.alias("p2"), [F.col("p1.user_left") == F.col("p2.subscription_user")], "inner"]\
                                       .join[user_subscription.alias("p3"), [F.col("p1.user_right") == F.col("p3.subscription_user")], "inner"]\
                                       .where("p2.subscription_channel == p3.subscription_channel")\
                                       .withColumn('processed_dttm', F.current_timestamp())\
                                       .select("user_left", "user_right", 'processed_dttm', F.col("city_id").alias('zone_id'), "local_time")


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
    calculate_user_rec_mart(path_event_prqt,path_city_data,spark)\
        .write.format('json').mode('overwrite').save(f'{path_mart_to_save}/user_mart_rec.json')


if __name__ == '__main__':
    main()


    
