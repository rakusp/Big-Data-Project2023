echo Making nifi dirs
hdfs dfs -mkdir -p /user/traffic/nifi/incidents_raw
hdfs dfs -mkdir /user/traffic/nifi/weather_raw
hdfs dfs -mkdir /user/traffic/nifi/pollution_raw
echo There should be 3 dirs listed now
hdfs dfs -ls /user/traffic/nifi
