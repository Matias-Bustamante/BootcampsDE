from pyspark.sql import SparkSession 
spark = SparkSession.builder \
    .master("spark://172.17.0.2:7077") \
    .enableHiveSupport() \
    .getOrCreate()

df=spark.read.parquet("hdfs://172.17.0.2:9000/buckets/yellow_tripdata_2021-01.parquet")
df_01=df.select(df["tpep_pickup_datetime"].cast("date"), df["airport_fee"], df["payment_type"].cast("int"), df["tolls_amount"].cast("float"), df["total_amount"].cast("float"))

df=spark.read.parquet("hdfs://172.17.0.2:9000/buckets/yellow_tripdata_2021-02.parquet") 
df_02=df.select(df["tpep_pickup_datetime"].cast("date"), df["airport_fee"], df["payment_type"].cast("int"), df["tolls_amount"].cast("float"), df["total_amount"].cast("float"))

data=df_01.union(df_02)

data=data.filter(data.payment_type==2)

data.write.insertInto("tripdata.airport_trips")

