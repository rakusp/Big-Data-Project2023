import sys
import logging
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, isnan, round, to_timestamp
from pyspark.sql.types import StringType, DateType
from datetime import datetime, timedelta

#configure logger
formatter = logging.Formatter('[%(asctime)s]%(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
incidents_categories = {"0": "unknown", "1": "Accident", "2": "Fog", "3": "Dangerous Conditions", "4": "Rain",
                        "5": "Ice", "6": "Jam", "7": "Lane Closed", "8": "Road Closed", "9": "Road Works", "10": "Wind",
                        "11": "Flooding", "14": "Broken Down Vehicle"}


def round_to_half_hour(timestamp):
    # return the nearest half hour for timestamp
    # in this project, half hour is half past some hour, e.g. 8:30
    date = datetime.fromtimestamp(timestamp)
    return datetime(date.year, date.month, date.day, date.hour, 30, 0)


def map_categories(mapping):
    # map category name to its id in incidents data
    def translate(col):
        return mapping.get(col)
    return udf(translate, StringType())


def merge_tables_from_hdfs(data):
    # create a single dataframe for all csv files from given HDFS directory
    return spark.read.option("header", "true").option("inferschema", "true").csv(
        "hdfs://localhost:8020/user/traffic/nifi/" + data)


def get_weather_and_pollution_data(cities=None, start_time=None, end_time=None):
    logger.info("Merging weather and pollution data...")
    df_pollution = merge_tables_from_hdfs("pollution_raw")
    df_weather = merge_tables_from_hdfs("weather_raw")
    cols_to_drop_weather = [colname for colname in df_weather.columns if colname.endswith('_units')]
    cols_to_drop_weather += ["generationtime_ms", "utc_offset_seconds", "timezone", "timezone_abbreviation", "interval"]
    df_weather = df_weather.drop(*cols_to_drop_weather)
    cols_to_drop_pollution = ['lon', 'lat']
    df_pollution = df_pollution.drop(*cols_to_drop_pollution)
    datetime_pollution_udf = udf(lambda x: str(round_to_half_hour(x)), StringType())
    df_pollution = df_pollution.withColumn("time", datetime_pollution_udf(col("timestamp")))
    datetime_weather_udf = udf(lambda x: str(datetime.strptime(x, '%Y-%m-%dT%H:%M')), StringType())
    df_weather = df_weather.withColumn("time", datetime_weather_udf(col("time")))
    df_merged = df_weather.join(df_pollution,
                                (df_weather.city == df_pollution.city) & (df_weather.time == df_pollution.time)).drop(
        df_pollution.time).drop(df_pollution.city)
    if cities is not None:
        df_merged = df_merged.filter(df_merged.city.isin(cities))
    if start_time is not None and end_time is not None:
        merged_udf = udf(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), DateType())
        df_merged = df_merged.withColumn("time", to_timestamp(df_merged.time, 'yyyy-MM-dd HH:mm:ss'))
        df_merged = df_merged.filter((start_time <= col("time")) & (end_time >= col("time")))
    df_merged = df_merged.sort(col("city"), col("time"))
    df_merged.createOrReplaceTempView("WeatherAndPollutionData")
    logger.info("Done")
    return df_merged


def get_sum_of_incidents_for_categories(start_time=None, end_time=None, cities=None):
    # return number of incidents for each category in each city that started in given time
    logger.info("Getting incidents in a period data...")
    columns_to_drop = ['lon', 'lat']
    df_incidents = merge_tables_from_hdfs("incidents_raw").drop(*columns_to_drop)
    df_incidents = df_incidents.withColumn("iconCat", df_incidents.iconCat.cast(StringType()))
    df_incidents = df_incidents.replace(to_replace=incidents_categories, subset=['iconCat'])
    if cities is not None:
        df_incidents = df_incidents.filter(df_incidents.city.isin(cities))
    if start_time is not None and end_time is not None:
        df_incidents = df_incidents.filter((start_time <= col("start")) & (end_time >= col("start")))
    # do not count the same incident more than once:
    df_incidents = df_incidents.dropDuplicates(['id'])
    df_incidents = df_incidents.groupBy("city", "iconCat").count()
    df_incidents = df_incidents.sort(col("city"), col("iconCat"))
    df_incidents = df_incidents.withColumnRenamed("iconCat", "category")
    df_incidents.createOrReplaceTempView("SumOfIncidentsForCategories")
    logger.info("Done")
    return df_incidents


def get_daily_weather_data(columns_list, cities=None, start_time=None, end_time=None, type='sum'):
    # get aggregated weather data fot selected cities between selected times, aggregated by chosen type
    logger.info("Getting daily weather data...")
    df_weather = merge_tables_from_hdfs("weather_raw")
    cols_to_drop_weather = [colname for colname in df_weather.columns if colname.endswith('_units')]
    cols_to_drop_weather += ["generationtime_ms", "utc_offset_seconds", "timezone", "timezone_abbreviation", "interval"]
    df_weather = df_weather.drop(*cols_to_drop_weather)
    datetime_weather_udf = udf(lambda x: str(datetime.strptime(x, '%Y-%m-%dT%H:%M').date()), StringType())
    df_weather = df_weather.withColumn("day", datetime_weather_udf(col("time")))
    columns_list_select = columns_list + ["city", "day", "time"]
    df_weather = df_weather.select(*columns_list_select)
    if cities is not None:
        df_weather = df_weather.filter(df_weather.city.isin(cities))
    if start_time is not None and end_time is not None:
        df_weather = df_weather.filter((df_weather.day >= start_time) & (df_weather.day <= end_time))
    if type == 'sum':
        df_weather = df_weather.groupBy(["city", "day"]).sum()
    if type == 'mean':
        df_weather = df_weather.groupBy(["city", "day"]).avg()
    if type == 'max':
        df_weather = df_weather.groupBy(["city", "day"]).max()
    for col_name in df_weather.columns:
        if col_name not in ["city", "day"]:
            df_weather = df_weather.withColumn(col_name, round(col(col_name), 3))
    df_weather = df_weather.sort(col("city"),col("day"))
    df_weather.createOrReplaceTempView("DailyWeatherData")
    logger.info("Done")
    return df_weather


def get_daily_pollution_data(columns_list, cities=None, start_time=None, end_time=None, type='mean'):
    # get aggregated pollution data fot selected cities between selected times, aggregated by chosen type
    logger.info("Getting daily pollution data...")
    df_pollution = merge_tables_from_hdfs("pollution_raw")
    cols_to_drop = ['lon', 'lat']
    df_pollution = df_pollution.drop(*cols_to_drop)
    datetime_pollution_udf = udf(lambda x: str(round_to_half_hour(x).date()), StringType())
    df_pollution = df_pollution.withColumn("day", datetime_pollution_udf(col("timestamp")))
    columns_list_select = columns_list + ["city", "day"]
    df_pollution = df_pollution.select(*columns_list_select)
    if cities is not None:
        df_pollution = df_pollution.filter(df_pollution.city.isin(cities))
    if start_time is not None and end_time is not None:
        df_pollution = df_pollution.filter((df_pollution.day >= start_time) & (df_pollution.day <= end_time))
    if type == 'sum':
        df_pollution = df_pollution.groupBy(["city", "day"]).sum()
    if type == 'mean':
        df_pollution = df_pollution.groupBy(["city", "day"]).avg()
    if type == 'max':
        df_pollution = df_pollution.groupBy(["city", "day"]).max()
    for col_name in df_pollution.columns:
        if col_name not in ["city", "day"]:
            df_pollution = df_pollution.withColumn(col_name, round(col(col_name), 3))
    df_pollution = df_pollution.sort(col("city"),col("day"))
    df_pollution.createOrReplaceTempView("DailyPollutionData")
    logger.info("Done")
    return df_pollution


