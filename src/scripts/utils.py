#Подключаем необходимые пакеты
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

#Функция для определения города происхождения события

def get_event_with_city(path_event_prqt: str, 
                    path_city_data: str, 
                    spark: pyspark.sql.SparkSession,) -> pyspark.sql.DataFrame:
    
    #Считываем данные по событиям
    events_geo = spark.read.parquet(path_event_prqt)\
                 .drop('city','id')\
                 .withColumn('event_id', F.monotonically_increasing_id())
    
    #Считываем данные по городам и переименуем атрибуты
    mapping = dict(zip(['lat', 'lng', 'id'], ['city_lat', 'city_lng', 'city_id']))

    df_city = spark.read.csv(path_city_data, sep = ";", header = True)
    
    df_city = df_city\
              .select([F.col(c).alias(mapping.get(c, c)) for c in df_city.columns])\
              .withColumn('city_lat', F.regexp_replace(F.col('city_lat'), ',', '.').cast('double')) \
              .withColumn('city_lng', F.regexp_replace(F.col('city_lng'), ',', '.').cast('double'))
    
    
    events_city = events_geo \
                  .crossJoin(df_city) \
                  .withColumn('diff', F.acos(F.sin(F.col('city_lat'))*F.sin(F.col('lat')) +                         F.cos(F.col('city_lat'))*F.cos(F.col('lat'))*F.cos(F.col('city_lng')-F.col('lon')))*F.lit(6371))\
                  .withColumn('rank', F.row_number().over(Window.partitionBy('event_id').orderBy(F.asc('diff'))))\
                  .filter('rank < 2')\
                  .drop('rank', 'diff', 'lat', 'lon')\
                  .persist()
    return events_city