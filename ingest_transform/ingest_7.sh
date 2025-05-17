rm -f /home/hadoop/landing/*.parquet

wget -P /home/hadoop/landing https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.parquet
wget -P /home/hadoop/landing https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-02.parquet

/home/hadoop/hadoop/bin/hdfs dfs -rm /buckets/*.parquet

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/*.parquet /buckets

echo 'Archivo enviado al buckets en hdfs'

