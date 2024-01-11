import spark_processing_batch
from pyspark.sql import SparkSession
from datetime import datetime
                        
if __name__ == '__main__':
    spark_processing_batch.spark = SparkSession.builder.appName("SparkDataProcessing").getOrCreate()
    spark_processing_batch.spark.sparkContext.setLogLevel("ERROR")
    a = spark_processing_batch.get_weather_and_pollution_data(cities=["Warsaw"], start_time=datetime(2023, 12, 29, 18, 30, 0), end_time=datetime(2023, 12, 31, 0, 0, 0))
    spark_processing_batch.spark.sql("SELECT city, time, temperature_2m, rain, pm2_5, co, o3 from WeatherAndPollutionData WHERE time <= CAST('2023-12-30 11:30:00' AS DATE)").show()	
    b = spark_processing_batch.get_sum_of_incidents_for_categories(cities = ["Warsaw", "London", "Luxembourg"])
    spark_processing_batch.spark.sql("SELECT * FROM SumOfIncidentsForCategories").show()
    c = spark_processing_batch.get_daily_weather_data(columns_list=["temperature_2m", "soil_moisture_0_1cm"], start_time=datetime(2023, 12, 28, 8, 30, 0), end_time=datetime(2023, 12, 31, 0, 0, 0), type="mean")
    spark_processing_batch.spark.sql("SELECT * FROM DailyWeatherData WHERE city IN ('Warsaw','Brasilia','London')").show()
    d = spark_processing_batch.get_daily_pollution_data(columns_list=["pm10","pm2_5","co","aqi"], cities=["Warsaw", "Bern","New Delhi"], start_time=datetime(2023, 12, 28, 8, 30, 0), end_time=datetime(2023, 12, 31, 0, 0, 0))
    spark_processing_batch.spark.sql("SELECT * FROM DailyPollutionData").show()
    a.write.options(header=True, delimeter=',').csv("/home/vagrant/Big-Data-Project2023/merged.csv")
    b.write.options(header=True, delimeter=',').csv("/home/vagrant/Big-Data-Project2023/incidents.csv")
    c.write.options(header=True, delimeter=',').csv("/home/vagrant/Big-Data-Project2023/weather.csv")
    d.write.options(header=True, delimeter=',').csv("/home/vagrant/Big-Data-Project2023/pollution.csv")
    