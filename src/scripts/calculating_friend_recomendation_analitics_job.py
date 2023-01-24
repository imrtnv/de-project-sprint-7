import os
import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

from datetime import datetime, timedelta
from pyspark.sql.functions import regexp_replace
import sys
import pyspark.sql.functions as F 
from pyspark.sql.window import Window 
from pyspark.sql.types import *
from math import radians, cos, sin, asin, sqrt

#comment: задаем все переменные далее по коду они будут обозначены где они используются
#sname = "imrtnv"  #sys.argv[1]
#hdfs_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020" #sys.argv[2]
#geo_path = "/user/master/data/geo/events/" #sys.argv[3]
#citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/"
#start_date = '2022-05-21' #sys.argv[4]
#depth = 28 #sys.argv[5]


#comment: Функция формирует list со список папок для загрузки  
def input_paths (start_date, depth):

    list_date = []

    for i in range(depth):
        list_date.append(f'{hdfs_path}{geo_path}date='+str((datetime.fromisoformat(start_date) - timedelta(days=i)).date()))

    return list_date

#Функция расчета расстояния
def get_distance(lon_a, lat_a, lon_b, lat_b):

    lon_a, lat_a, lon_b, lat_b = map(radians, [lon_a,  lat_a, lon_b, lat_b])
    dist_longit = lon_b - lon_a
    dist_latit = lat_b - lat_a

    area = sin(dist_latit/2)**2 + cos(lat_a) * cos(lat_b) * sin(dist_longit/2)**2

    central_angle = 2 * asin(sqrt(area))
    radius = 6371

    distance = central_angle * radius

    return abs(round(distance, 2))

#Создадим пользовательскую функцию, чтобы использовать ее в нашем Spark DataFrame
udf_get_distance = F.udf(get_distance)


def main():
    spark = (
            SparkSession
            .builder
            .master('yarn')
            .appName(f"{sname}_calculating_friend_recomendation_{date}")
            .getOrCreate()
        )
    

    #Получаем все подписки и удаляем дубликаты
    df_all_subscriptions = (spark.read.parquet(*input_paths(start_date, depth))
        .filter("event_type == 'subscription'")
        .where((F.col('event.subscription_channel').isNotNull() & F.col('event.user').isNotNull()))
        .select(F.col('event.subscription_channel').alias('channel_id'),F.col('event.user').alias('user_id')).
        distinct())


    cols = ['user_left', 'user_right']
    #Перемножаем подписки (делаем иннер джоин по channel_id)
    df_subscriptions = (df_all_subscriptions.withColumnRenamed("user_id", "user_left")
        .join(df_all_subscriptions.withColumnRenamed("user_id", "user_right"), on="channel_id", how='inner')
        .withColumn("arr",F.array_sort(F.array(*cols)))
        .drop_duplicates(["arr"]).drop("channel_id", "arr")
        .filter(F.col("user_left") != F.col("user_right"))
        .withColumn("hash", F.hash(F.concat(F.col('user_left'),F.col('user_right')))))


    #создаем df по людям которые общались (переписывались - имеют пары message_from message_to и наоборот)
    #считываем из источника input_event_message_paths в df_user_messages
    #df_user_message_from_to - левая стора общения
    #объединяем df_user_message_from_to и df_user_message_to_from = df_user_communications
    df_user_messages_from_to = (spark.read.parquet(*input_paths(start_date, depth))
        .filter("event_type == 'message'")
        .where((F.col('event.message_from').isNotNull()&F.col('event.message_to').isNotNull()))
        .select(F.col('event.message_from').alias('user_left'),F.col('event.message_to').alias('user_right'))
        .distinct())

    df_user_messages_to_from = (spark.read.parquet(*input_paths(start_date, depth))
        .filter("event_type == 'message'")
        .where((F.col('event.message_from').isNotNull()&F.col('event.message_to').isNotNull()))
        .select(F.col('event.message_to').alias('user_left'),F.col('event.message_from').alias('user_right'))
        .distinct())

    #делаю добавление левых к правым и правых к левым потому что не известно какая комбинация встретится в df_subscriptions
    #filter(F.col("user_left") != F.col("user_right") ) - удаляем пользователей где левый равен правому
    df_user_communications = (df_user_messages_from_to.union(df_user_messages_to_from)
        .withColumn("arr", F.array_sort(F.array(*cols)))
        .drop_duplicates(["arr"])
        .drop("arr")
        .withColumn("hash", F.hash(F.concat(F.col('user_left'),F.col('user_right'))))
        .filter(F.col("user_left") != F.col("user_right")))

    df_subscriptions_without_communication = (df_subscriptions.join(df_user_communications
        .withColumnRenamed("user_right", "user_right_temp")
        .withColumnRenamed("user_left", "user_left_temp"), on=["hash"], how='left')
        .where(F.col("user_right_temp").isNull())
        .drop("user_right_temp","user_left_temp", "hash")
        .where(F.col("user_left") != 0)
        .filter(F.col("user_left") != F.col("user_right")))

    #Получаем все подписки и удаляем дубликаты
    df_events_messages = (spark.read.parquet(*input_paths(start_date, depth))
        .filter("event_type == 'message'")
        .where( F.col("lat").isNotNull() | (F.col("lon").isNotNull()))
        .select(F.col('event.message_from')
        .alias('user_id'),F.col("lat").alias('lat'),F.col("lon").alias('lon'))
        .distinct())


    #Получение подписок и координат с округлением до двух знаков в дробной части
    df_events_subscription = (spark.read.parquet(*input_paths(start_date, depth))
        .filter("event_type == 'subscription'")
        .where( F.col("lat").isNotNull() | (F.col("lon").isNotNull()))
        .select(F.col("event.user")
        .alias('user_id'),F.col("lat").alias('lat'),F.col("lon").alias('lon'))
        .distinct())

    #объединение координат сообщений и подписок
    df_events_coordinats = (df_events_subscription.union(df_events_messages).distinct())

    #Создаем df_events_subscription_coordinat с подписками и координатами на основе 
    df_events_subscription_coordinat = (
        df_subscriptions_without_communication
            .join(df_events_coordinats
                .withColumnRenamed("user_id", "user_left")
                .withColumnRenamed("lon", "lon_left")
                .withColumnRenamed("lat", "lat_left")
                , on=["user_left"] , how="inner")
            .join(df_events_coordinats
                .withColumnRenamed("user_id", "user_right")
                .withColumnRenamed("lon", "lon_right")
                .withColumnRenamed("lat", "lat_right")
                , on=["user_right"] , how="inner"))

    #Считаем дистаницию df_distance - фильтруем и оставляем только те у которых расстояние <= 1км 
    df_distance = (df_events_subscription_coordinat.withColumn("distance", udf_get_distance(F.col("lon_left"), F.col("lat_left"), F.col("lon_right"), F.col("lat_right")).cast(DoubleType()))
        .where(F.col("distance") <= 1.0 )
        .withColumnRenamed("lat_left", "lat")
        .withColumnRenamed("lon_left", "lon")
        .drop("lat_right", "lon_right", "distance"))


    #citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/geo.csv"
    df_csv = spark.read.csv(citygeodata_csv, sep = ';', header = True)
    df_csv = df_csv.withColumn("lat",regexp_replace("lat", ",", ".")).withColumn("lng",regexp_replace("lng",",","."))

    #Оставлю только два крупных города (так как действительно вы писали что мое решение будет падать, но и в вашем готовым решении он не нашел часть городов из файла)
    df_citygeodata = df_citygeodata.filter(F.col('city_id')<3)

    #Изменим тип и название столбцов
    df_citygeodata = (df_citygeodata.select(F.col("id")
        .cast(LongType()).alias("city_id"),(F.col("city")).alias("city_name"),(F.col("lat")).cast(DoubleType())
        .alias("city_lat"),(F.col("lng")).cast(DoubleType()).alias("city_lon")))

    #Перемножаем на координаты городов df_user_city (так как растояние 1 км между пользователями значит они находятся в одном городе и множно брать координаты одного человека для вычисления zone_id)
    #Считаем расстояние до города df_distance_city для вычисления zone_id фильтруем чтобы получить только один город для связки user_left; user_right
    df_user_city = (df_distance.crossJoin(df_citygeodata.hint("broadcast"))\
        .withColumn("distance", udf_get_distance(F.col("lon"), F.col("lat"), F.col("city_lon"), F.col("city_lat"))
        .cast(DoubleType()))
            .withColumn("row" ,F.row_number().over(Window.partitionBy("user_left", "user_right")
                                               .orderBy(F.col("distance").asc())))
        .filter(F.col("row") == 1)
        .drop("row","lon", "lat", "city_lon", "city_lat", "distance", "channel_id")
        .withColumnRenamed("city_id", "zone_id")
        .distinct())

    #Формируем витрину
    df_friend_recomendation_analitics_mart = (df_user_city
        .withColumn("processed_dttm" , F.current_date())
        .withColumn("timezone", F.concat(F.lit("Australia/"), F.col('city_name')))
        .withColumn("local_time", F.from_utc_timestamp(F.col("processed_dttm"),F.col('timezone')))
        .drop("timezone", "city_name"))

    #comment: Сохранение витрины для аналитиков на hdfs 
    (df_friend_recomendation_analitics_mart.write
            .mode("overwrite")
            .parquet(f"{hdfs_path}/user/{sname}/marts/friend_recomendation"))


if __name__ == '__main__':
    main()